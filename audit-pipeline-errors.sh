#!/usr/bin/env bash
# audit-pipeline-errors.sh
#
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 nara
#
# One-shot audit of errors / skips / filters across a Capsule pipeline.
# Uses only in-cluster access (kubectl + port-forward). No Basic Auth needed.
#
# Output:
#   - Human-readable report on stdout
#   - Raw logs + per-section artifacts under $OUTDIR (set in config)
#
# Usage:
#   audit-pipeline-errors.sh -c /path/to/audit.conf
#   CAPSULE_AUDIT_CONFIG=/path/to/audit.conf  audit-pipeline-errors.sh
#   audit-pipeline-errors.sh                  # auto-uses ./audit.conf or
#                                             # audit.conf next to the script
#
# A template config file ships alongside the script as `audit.conf.example`.
# All machine-/deployment-specific values (namespace, pod labels,
# DB users, ports, audit paths, etc.) live there, so this script is
# generic across Capsule installations.
#
# Environment overrides (take precedence over the config file):
#   OUTDIR, CSV_FILE, NS, KUBECTL, ETL_PORT  (handy for one-off runs)

set -u
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=audit-config-lib.sh
. "$SCRIPT_DIR/audit-config-lib.sh"

load_audit_config "$@" || exit 1
set -- "${_AUDIT_REMAINING_ARGS[@]}"

# ---------- runtime setup ----------
mkdir -p "$OUTDIR"

# Wipe any stale artifacts from a previous run so this run's output is clean.
# (We intentionally keep the directory itself — useful if something else cd's into it.)
find "$OUTDIR" -mindepth 1 -maxdepth 1 -type f -delete 2>/dev/null || true

# ---------- helpers ----------
c_red()   { printf "\033[31m%s\033[0m" "$*"; }
c_grn()   { printf "\033[32m%s\033[0m" "$*"; }
c_yel()   { printf "\033[33m%s\033[0m" "$*"; }
c_bld()   { printf "\033[1m%s\033[0m"  "$*"; }
section() { echo; echo "============================================================"; c_bld "$1"; echo; echo "============================================================"; }
sub()     { echo; c_yel "--- $1 ---"; echo; }

cleanup() {
  if [[ -n "${PF_PID:-}" ]] && kill -0 "$PF_PID" 2>/dev/null; then
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

require_bin() { command -v "$1" >/dev/null 2>&1 || { echo "Missing required binary: $1"; exit 1; }; }
require_bin "$KUBECTL"
require_bin awk
require_bin sed
require_bin grep
require_bin sort
require_bin uniq
HAS_JQ=1;   command -v jq   >/dev/null 2>&1 || HAS_JQ=0
HAS_CURL=1; command -v curl >/dev/null 2>&1 || HAS_CURL=0

echo "Using config: ${AUDIT_CONFIG_PATH}"
echo "Namespace   : ${NS}"
echo "Output dir  : ${OUTDIR}"

# ---------- discover pods ----------
section "0. Discover pods in namespace '$NS'"
"$KUBECTL" -n "$NS" get pods -o wide | tee "$OUTDIR/pods.txt"

ETL_APP_POD=$("$KUBECTL" -n "$NS" get pods -l "$ETL_APP_SELECTOR"  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
ETL_DB_POD=$("$KUBECTL"  -n "$NS" get pods -l "$ETL_DB_SELECTOR"   -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
OMOP_ETL_POD=$("$KUBECTL" -n "$NS" get pods -l "$OMOP_ETL_SELECTOR" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
OMOP_DB_POD=$("$KUBECTL"  -n "$NS" get pods --no-headers 2>/dev/null | awk -v p="$OMOP_DB_POD_PATTERN" '$1 ~ p {print $1; exit}')
AERO_POD=$("$KUBECTL"     -n "$NS" get pods --no-headers 2>/dev/null | awk -v p="$AERO_POD_PATTERN"    '$1 ~ p {print $1; exit}')

echo
echo "etl-idea (app)    : ${ETL_APP_POD:-<not found>}   (selector: $ETL_APP_SELECTOR)"
echo "etl postgres      : ${ETL_DB_POD:-<not found>}    (selector: $ETL_DB_SELECTOR)"
echo "omop-etl          : ${OMOP_ETL_POD:-<not found>}  (selector: $OMOP_ETL_SELECTOR)"
echo "omop-cdm postgres : ${OMOP_DB_POD:-<not found>}   (pattern:  $OMOP_DB_POD_PATTERN)"
echo "aerospike         : ${AERO_POD:-<not found>}      (pattern:  $AERO_POD_PATTERN)"

# ---------- 1. Upload / etl-idea app errors ----------
section "1. Upload & etl-idea app errors"

if [[ -n "${ETL_APP_POD}" ]]; then
  "$KUBECTL" -n "$NS" logs -l "$ETL_APP_SELECTOR" --tail=500000 > "$OUTDIR/etl-idea.log" 2>/dev/null || true
  echo "Saved etl-idea log: $OUTDIR/etl-idea.log ($(wc -l < "$OUTDIR/etl-idea.log") lines)"

  sub "HTTP-level errors (413 / 4xx / 5xx / MultipartException)"
  grep -Ei '413|Payload Too Large|MaxUploadSizeExceeded|MultipartException|IOException|400 Bad Request| 401 | 403 | 500 ' \
      "$OUTDIR/etl-idea.log" | tail -50 || true
  HTTP_ERR=$(grep -Eci '413|Payload Too Large|MaxUploadSizeExceeded|MultipartException' "$OUTDIR/etl-idea.log" || true)
  echo
  echo "HTTP-level error lines: $HTTP_ERR"

  sub "Top ERROR / Exception lines"
  grep -E ' ERROR | FATAL |Exception' "$OUTDIR/etl-idea.log" \
    | sed -E 's/^[0-9:\.\- TZ]+[[:space:]]+//' \
    | awk '{$1=""; print}' \
    | sort | uniq -c | sort -rn | head -20 || true

  sub "Pod restarts / OOM / crashes"
  "$KUBECTL" -n "$NS" describe pods -l "$ETL_APP_SELECTOR" \
    | grep -E 'Restart Count|Last State|Reason:|Message:|OOMKilled' | head -40 || true
else
  c_red "etl-idea pod not found — skipping Stage 1."
  echo
fi

# ---------- 2. Staging errors ----------
section "2. Staging stage (Aerospike $AERO_SET_ERRORS + $ETL_DB_PATIENT_TABLE)"

if [[ -n "${AERO_POD}" ]]; then
  sub "Aerospike set sizes"
  "$KUBECTL" -n "$NS" exec "$AERO_POD" -- sh -lc "
    echo \"[${AERO_SET_RECORDS}]\"; asinfo -v \"sets/${AERO_NAMESPACE}/${AERO_SET_RECORDS}\" 2>/dev/null | tr ':' '\n';
    echo;
    echo \"[${AERO_SET_ERRORS}]\";  asinfo -v \"sets/${AERO_NAMESPACE}/${AERO_SET_ERRORS}\"  2>/dev/null | tr ':' '\n'
  " | tee "$OUTDIR/aerospike-sets.txt" || true
else
  c_yel "aerospike pod not found — skipping Aerospike summary."
  echo
fi

# Try to hit the audit API via port-forward (no auth required in-cluster).
# On this build the endpoints are:
#   GET $AUDIT_ERRORS_PATH  -> plain JSON array (all errors, pagination ignored)
#   GET $AUDIT_RECORDS_PATH -> plain JSON array (all processed records)
# Both responses can be large (100+ MB).
if [[ -n "${ETL_APP_POD}" && $HAS_CURL -eq 1 ]]; then
  sub "audit API via port-forward (no auth needed)"
  "$KUBECTL" -n "$NS" port-forward "$ETL_SERVICE" "${ETL_PORT}:${ETL_PORT}" >/dev/null 2>&1 &
  PF_PID=$!
  sleep 3

  AUDIT_URL="http://127.0.0.1:${ETL_PORT}${AUDIT_ERRORS_PATH}"

  if curl -fs "$AUDIT_URL" -o "$OUTDIR/etl-errors.json"; then
    SIZE=$(wc -c < "$OUTDIR/etl-errors.json")
    echo "Saved etl-errors.json (${SIZE} bytes)"

    if [[ $HAS_JQ -eq 1 ]]; then
      # The endpoint returns a plain JSON array on this build.
      # Be tolerant of a Spring-Data-style Page response too.
      TOTAL=$(jq -r '
        if type=="array" then length
        elif type=="object" and (.totalElements // empty) then .totalElements
        elif type=="object" and (.content // empty) then (.content|length)
        else 0 end
      ' "$OUTDIR/etl-errors.json" 2>/dev/null || echo 0)
      echo "Total etl-errors: ${TOTAL}"

      # Extract array of error items regardless of wrapper shape.
      ITEMS_FILTER='if type=="array" then . elif type=="object" and (.content // empty) then .content else [] end'

      echo
      echo "By error code:"
      jq -r "${ITEMS_FILTER} | .[] | .error // \"(none)\"" "$OUTDIR/etl-errors.json" \
        | sort | uniq -c | sort -rn | tee "$OUTDIR/etl-errors-by-code.txt" | head -20 || true

      echo
      echo "By resourceName (which CSV section the error belongs to):"
      jq -r "${ITEMS_FILTER} | .[] | .resourceName // \"(none)\"" "$OUTDIR/etl-errors.json" \
        | sort | uniq -c | sort -rn | tee "$OUTDIR/etl-errors-by-resource.txt" | head -20 || true

      # Resource-instance-level error counts (distinct recordId per
      # resourceName). Drives the per-resource reconciliation in the
      # summarizer — a resource instance is "errored" if ANY of its
      # fields errored.
      jq -r "${ITEMS_FILTER} | .[] | \"\(.recordId)\t\(.resourceName // \"(none)\")\"" \
        "$OUTDIR/etl-errors.json" \
        | sort -u \
        | awk -F'\t' 'NF==2 && $2!="(none)" {r[$2]++} END{for(k in r) printf "%7d %s\n", r[k], k}' \
        | sort -rn \
        | tee "$OUTDIR/etl-errors-by-resource-instances.txt" >/dev/null
      echo "Errored-instance breakdown: $OUTDIR/etl-errors-by-resource-instances.txt"

      echo
      echo "By propertyInError (which field failed):"
      jq -r "${ITEMS_FILTER} | .[] | .propertyInError // \"(none)\"" "$OUTDIR/etl-errors.json" \
        | sort | uniq -c | sort -rn | tee "$OUTDIR/etl-errors-by-property.txt" | head -20 || true

      echo
      echo "Top motivation prefixes (first 120 chars):"
      jq -r "${ITEMS_FILTER} | .[] | (.motivation // .errorMessage // .message // \"(no message)\") | .[0:120]" \
        "$OUTDIR/etl-errors.json" \
        | sort | uniq -c | sort -rn | tee "$OUTDIR/etl-errors-by-motivation.txt" | head -20 || true

      echo
      echo "Sample rows (first 10, TSV: error | resourceName | propertyInError | motivation):"
      jq -r "${ITEMS_FILTER} | .[0:10] | .[] |
        [.error // \"-\", .resourceName // \"-\", .propertyInError // \"-\",
         ((.motivation // .errorMessage // .message // \"-\") | .[0:160])]
        | @tsv" "$OUTDIR/etl-errors.json" \
        | tee "$OUTDIR/etl-errors-samples.tsv" || true
    else
      echo "(install jq for a detailed breakdown)"
      head -c 400 "$OUTDIR/etl-errors.json"; echo
    fi
  else
    c_yel "Could not reach $AUDIT_URL. Probing alternative paths..."
    {
      echo "# Probe run at $(date)"
      for p in \
        "$AUDIT_ERRORS_PATH" \
        "/etl${AUDIT_ERRORS_PATH}" \
        "$AUDIT_RECORDS_PATH" \
        "/etl${AUDIT_RECORDS_PATH}" \
        /audit \
        /v3/api-docs; do
        code=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:${ETL_PORT}${p}")
        echo "$code  $p"
      done
    } > "$OUTDIR/etl-errors.probe"
    echo "Wrote probe results to $OUTDIR/etl-errors.probe"
  fi

  # Also fetch /audit/records (processed rows) for completeness
  RECORDS_URL="http://127.0.0.1:${ETL_PORT}${AUDIT_RECORDS_PATH}"
  if curl -fs "$RECORDS_URL" -o "$OUTDIR/etl-records.json"; then
    RSIZE=$(wc -c < "$OUTDIR/etl-records.json")
    echo
    echo "Saved etl-records.json (${RSIZE} bytes)"
    if [[ $HAS_JQ -eq 1 ]]; then
      RTOTAL=$(jq -r '
        if type=="array" then length
        elif type=="object" and (.totalElements // empty) then .totalElements
        elif type=="object" and (.content // empty) then (.content|length)
        else 0 end
      ' "$OUTDIR/etl-records.json" 2>/dev/null || echo 0)
      echo "Total etl-records: ${RTOTAL}"

      # Per-resource INSTANCE counts — used for cross-layer
      # reconciliation against staging table counts. Each entry in
      # etl-records.json is one field of one resource instance
      # (coreVariable = "Patient.birthDate", "Surgery.type", ...), so
      # we group by recordId and take the prefix of coreVariable as
      # the resource name. One row per distinct (recordId, resource)
      # pair = one resource instance.
      ITEMS_FILTER='if type=="array" then . elif type=="object" and (.content // empty) then .content else [] end'
      jq -r "${ITEMS_FILTER} | .[] | \"\(.recordId)\t\(.coreVariable // \"(none)\" | split(\".\")[0])\"" \
        "$OUTDIR/etl-records.json" \
        | sort -u \
        | awk -F'\t' 'NF==2 && $2!="(none)" {r[$2]++} END{for(k in r) printf "%7d %s\n", r[k], k}' \
        | sort -rn \
        | tee "$OUTDIR/etl-records-by-resource.txt" >/dev/null
      echo "Per-resource instance breakdown: $OUTDIR/etl-records-by-resource.txt"
    fi
  fi

  cleanup
  unset PF_PID
fi

# Staging table state
if [[ -n "${ETL_DB_POD}" ]]; then
  sub "Staging table row counts (public.* in '${ETL_DB_NAME}' db)"
  "$KUBECTL" exec -n "$NS" "$ETL_DB_POD" -- psql -U "$ETL_DB_USER" -d "$ETL_DB_NAME" -c "
    SELECT table_name,
           (xpath('/row/c/text()',
                  query_to_xml(format('SELECT COUNT(*) AS c FROM public.%I', table_name),
                               true, true, '')))[1]::text::bigint AS row_count
    FROM information_schema.tables
    WHERE table_schema='public' AND table_type='BASE TABLE'
    ORDER BY row_count DESC, table_name;
  " 2>/dev/null | tee "$OUTDIR/staging-counts.txt" || true

  STAGING_PATIENTS=$("$KUBECTL" exec -n "$NS" "$ETL_DB_POD" -- \
      psql -U "$ETL_DB_USER" -d "$ETL_DB_NAME" \
           -tAc "SELECT COUNT(*) FROM ${ETL_DB_PATIENT_TABLE};" 2>/dev/null || echo "?")
  echo "staging_patients=${STAGING_PATIENTS}"
else
  c_yel "etl postgres pod not found — skipping staging counts."
  STAGING_PATIENTS="?"
fi

# CSV reconciliation
if [[ -n "$CSV_FILE" && -f "$CSV_FILE" ]]; then
  CSV_ROWS=$(($(wc -l < "$CSV_FILE") - 1))
  echo "csv_rows (excluding header): ${CSV_ROWS}"
  echo "${CSV_ROWS} / ${STAGING_PATIENTS} / (errors see above)"
fi

# ---------- 3. OMOP ETL errors ----------
section "3. OMOP ETL stage (omop-etl Spring Batch job)"

if [[ -n "${OMOP_ETL_POD}" ]]; then
  "$KUBECTL" -n "$NS" logs -l "$OMOP_ETL_SELECTOR" --tail=5000000 > "$OUTDIR/omop-etl.log" 2>/dev/null || true
  LINES=$(wc -l < "$OUTDIR/omop-etl.log")
  echo "Saved omop-etl log: $OUTDIR/omop-etl.log (${LINES} lines)"

  sub "Job status"
  grep -E 'Job: \[|Status=|BatchStatus=|Starting job|Job completed|Job failed' \
      "$OUTDIR/omop-etl.log" | tail -20 || true

  sub "Severe (non-skip) errors"
  grep -E ' ERROR | FATAL |SQLException|PSQLException|DataAccessException|BeanCreationException|FlywayException' \
      "$OUTDIR/omop-etl.log" \
    | grep -v 'EXCEPTION SKIP' > "$OUTDIR/omop-severe.txt" || true
  SEVERE=$(wc -l < "$OUTDIR/omop-severe.txt")
  echo "Severe lines: ${SEVERE} (see $OUTDIR/omop-severe.txt)"
  head -15 "$OUTDIR/omop-severe.txt" || true

  sub "STEP_COUNT summary (per-step read / write / filter / skip)"
  grep 'STEP_COUNT' "$OUTDIR/omop-etl.log" \
    | sed -E 's/.*step=([A-Za-z]+) read=([0-9]+) write=([0-9]+) filter=([0-9]+) skip=([0-9]+).*/\1\t\2\t\3\t\4\t\5/' \
    | awk 'BEGIN{printf "%-45s %8s %8s %8s %6s\n","STEP","READ","WRITE","FILTER","SKIP"}
           {printf "%-45s %8s %8s %8s %6s\n",$1,$2,$3,$4,$5}' \
    | tee "$OUTDIR/omop-step-counts.txt" || true

  sub "Steps that filtered 100% (read>0 but write=0)"
  awk 'NR>1 && $2>0 && $3==0 {print}' "$OUTDIR/omop-step-counts.txt" || true

  sub "Steps with any skip>0"
  awk 'NR>1 && $5>0 {print}' "$OUTDIR/omop-step-counts.txt" || true

  sub "EXCEPTION SKIP — counts by exception type + step"
  grep 'EXCEPTION SKIP' "$OUTDIR/omop-etl.log" \
    | sed -E 's/.*exception=([A-Za-z]+) .*step=([A-Za-z]+).*/\1\t\2/' \
    | sort | uniq -c | sort -rn | tee "$OUTDIR/omop-skip-by-type.txt" || true

  sub "EXCEPTION SKIP — distinct source IDs per step"
  grep 'EXCEPTION SKIP' "$OUTDIR/omop-etl.log" \
    | sed -E 's/.*step=([A-Za-z]+) item=[^\[]+\[(id|sourceId)=([0-9]+).*/\1 \3/' \
    | sort -u > "$OUTDIR/omop-skip-ids.txt" || true
  echo "Distinct (step,id) pairs skipped: $(wc -l < "$OUTDIR/omop-skip-ids.txt")"
  head -20 "$OUTDIR/omop-skip-ids.txt" || true
else
  c_yel "omop-etl pod not found — skipping Stage 3."
  echo
fi

# ---------- 4. Handshake: staging vs OMOP ----------
section "4. Handshake — staging vs OMOP"

if [[ -n "${OMOP_DB_POD}" ]]; then
  "$KUBECTL" exec -i -n "$NS" "$OMOP_DB_POD" -- psql -U "$OMOP_DB_USER" -d "$OMOP_DB_NAME" -c "
    SELECT
      (SELECT COUNT(*) FROM ${OMOP_SCHEMA}.person)               AS person,
      (SELECT COUNT(*) FROM ${OMOP_SCHEMA}.visit_occurrence)     AS visit_occurrence,
      (SELECT COUNT(*) FROM ${OMOP_SCHEMA}.condition_occurrence) AS condition_occurrence,
      (SELECT COUNT(*) FROM ${OMOP_SCHEMA}.procedure_occurrence) AS procedure_occurrence,
      (SELECT COUNT(*) FROM ${OMOP_SCHEMA}.measurement)          AS measurement,
      (SELECT COUNT(*) FROM ${OMOP_SCHEMA}.observation)          AS observation,
      (SELECT COUNT(*) FROM ${OMOP_SCHEMA}.episode)              AS episode,
      (SELECT COUNT(*) FROM ${OMOP_SCHEMA}.drug_exposure)        AS drug_exposure,
      (SELECT COUNT(*) FROM ${OMOP_SCHEMA}.death)                AS death;
  " 2>/dev/null | tee "$OUTDIR/omop-counts.txt" || true

  OMOP_PERSONS=$("$KUBECTL" exec -i -n "$NS" "$OMOP_DB_POD" -- \
      psql -U "$OMOP_DB_USER" -d "$OMOP_DB_NAME" \
           -tAc "SELECT COUNT(*) FROM ${OMOP_SCHEMA}.person;" 2>/dev/null || echo "?")
  echo
  echo "staging_patients : ${STAGING_PATIENTS}"
  echo "omop_persons     : ${OMOP_PERSONS}"
  if [[ "$STAGING_PATIENTS" == "$OMOP_PERSONS" && "$STAGING_PATIENTS" != "?" ]]; then
    c_grn "MATCH ✓"
    echo
  else
    c_red "MISMATCH ✗"
    echo
  fi
else
  c_yel "omop-cdm postgres pod not found — skipping OMOP counts."
  echo
fi

# ---------- 5. Final summary ----------
section "5. Summary"

UPLOAD_ERR="${HTTP_ERR:-0}"

STAGING_ERR="?"
if [[ -f "$OUTDIR/etl-errors.json" && $HAS_JQ -eq 1 ]]; then
  STAGING_ERR=$(jq -r '
    if type=="array" then length
    elif type=="object" and (.totalElements // empty) then .totalElements
    elif type=="object" and (.content // empty) then (.content|length)
    else 0 end
  ' "$OUTDIR/etl-errors.json" 2>/dev/null || echo 0)
fi

STAGING_RECORDS="?"
if [[ -f "$OUTDIR/etl-records.json" && $HAS_JQ -eq 1 ]]; then
  STAGING_RECORDS=$(jq -r '
    if type=="array" then length
    elif type=="object" and (.totalElements // empty) then .totalElements
    elif type=="object" and (.content // empty) then (.content|length)
    else 0 end
  ' "$OUTDIR/etl-records.json" 2>/dev/null || echo 0)
fi

OMOP_SKIP=0
[[ -f "$OUTDIR/omop-skip-by-type.txt" ]] && OMOP_SKIP=$(awk '{s+=$1} END{print s+0}' "$OUTDIR/omop-skip-by-type.txt")
OMOP_SEVERE=0
[[ -f "$OUTDIR/omop-severe.txt" ]] && OMOP_SEVERE=$(wc -l < "$OUTDIR/omop-severe.txt")

printf "%-40s %s\n" "Stage 1 (upload) HTTP errors:"   "$UPLOAD_ERR"
printf "%-40s %s\n" "Stage 2 (staging) records:"      "$STAGING_RECORDS"
printf "%-40s %s\n" "Stage 2 (staging) row errors:"   "$STAGING_ERR"
printf "%-40s %s\n" "Stage 3 (OMOP) EXCEPTION SKIPs:" "$OMOP_SKIP"
printf "%-40s %s\n" "Stage 3 (OMOP) severe errors:"   "$OMOP_SEVERE"
printf "%-40s %s\n" "Staging patients:"                "$STAGING_PATIENTS"
printf "%-40s %s\n" "OMOP persons:"                    "${OMOP_PERSONS:-?}"
echo
echo "All artifacts saved under: $OUTDIR"
ls -la "$OUTDIR"
