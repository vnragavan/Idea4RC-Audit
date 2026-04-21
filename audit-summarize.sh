#!/usr/bin/env bash
# audit-summarize.sh
#
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 nara
#
# Generate a human-readable summary of an audit run produced by
# audit-pipeline-errors.sh.
#
# The script is fully OFFLINE — it only reads the artifact files,
# never touches the cluster. Re-run as many times as you want.
#
# Usage:
#   audit-summarize.sh -c /path/to/audit.conf
#       # Read $OUTDIR from the config file.
#
#   audit-summarize.sh -c audit.conf /path/to/audit-dir
#       # Override $OUTDIR with a specific run directory.
#
#   audit-summarize.sh -c audit.conf -m
#       # Also write SUMMARY.md alongside artifacts.
#
#   audit-summarize.sh -c audit.conf -o report.md /path
#       # Write plain markdown to report.md.
#
# The config file makes this script generic across Capsule installations.
# A template ships alongside the script as `audit.conf.example`.
# CAPSULE_AUDIT_CONFIG and a script-adjacent `audit.conf` are also honored.

set -u
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=audit-config-lib.sh
. "$SCRIPT_DIR/audit-config-lib.sh"

load_audit_config "$@" || exit 1
set -- "${_AUDIT_REMAINING_ARGS[@]}"

# ---------- args (remaining, after config loader extracted -c) ----------
WRITE_MD=0
MD_OUT=""
POS_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -m|--markdown)   WRITE_MD=1; shift ;;
    -o|--output)     MD_OUT="$2"; shift 2 ;;
    -h|--help)
      grep -E '^# ' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) POS_ARGS+=("$1"); shift ;;
  esac
done

# Positional dir overrides the OUTDIR from the config (handy for auditing
# old runs without editing the config).
DIR="${POS_ARGS[0]:-$OUTDIR}"

if [[ ! -d "$DIR" ]]; then
  echo "Audit directory not found: $DIR" >&2
  exit 1
fi

# If -m was given but no -o, default the markdown path to SUMMARY.md inside $DIR
if [[ $WRITE_MD -eq 1 && -z "$MD_OUT" ]]; then
  MD_OUT="$DIR/SUMMARY.md"
fi

# ---------- helpers ----------
# When we emit output we may also capture to an MD file (without ANSI colors).
_md=""
_md_mode=0
_plain=""

bold()  { printf "\033[1m%s\033[0m" "$*"; }
green() { printf "\033[32m%s\033[0m" "$*"; }
red()   { printf "\033[31m%s\033[0m" "$*"; }
yellow(){ printf "\033[33m%s\033[0m" "$*"; }
dim()   { printf "\033[2m%s\033[0m" "$*"; }

# Emit a line to both stdout (colored) and to the markdown buffer (plain).
emit() { echo -e "$*"; _plain+="$(echo -e "$*" | sed -E 's/\x1b\[[0-9;]*m//g')"$'\n'; }
h1()   { emit; emit "$(bold "# $*")"; emit; }
h2()   { emit; emit "$(bold "## $*")"; emit; }
h3()   { emit; emit "$(bold "### $*")"; emit; }
kv()   { emit "- **$1**: $2"; }

# Read a file if it exists, else echo empty string.
f() { [[ -f "$1" ]] && cat "$1" || echo ""; }

# Count file lines (0 if missing).
lc() { [[ -f "$1" ]] && wc -l < "$1" | tr -d ' ' || echo 0; }

# Pretty number with thousands separators.
fmt() { printf "%'d" "${1:-0}" 2>/dev/null || echo "${1:-0}"; }

# Extract a number from a file (first digits on a matching line).
num_from() {
  local file="$1" pattern="$2" default="${3:-0}"
  [[ -f "$file" ]] || { echo "$default"; return; }
  grep -E "$pattern" "$file" | head -1 | grep -Eo '[0-9]+' | head -1 || echo "$default"
}

# JSON array length if jq available and file exists.
json_len() {
  local file="$1"
  [[ -f "$file" ]] || { echo 0; return; }
  command -v jq >/dev/null 2>&1 || { echo "?"; return; }
  jq -r '
    if type=="array" then length
    elif type=="object" and (.totalElements // empty) then .totalElements
    elif type=="object" and (.content // empty) then (.content|length)
    else 0 end
  ' "$file" 2>/dev/null || echo 0
}

# Top N from a "count value" formatted file (output of `sort | uniq -c | sort -rn`).
top_n() {
  local file="$1" n="$2"
  [[ -f "$file" ]] || return
  head -n "$n" "$file"
}

# Render a fenced code block.
code_block() {
  local content="$1" lang="${2:-}"
  emit "\`\`\`${lang}"
  while IFS= read -r line; do emit "$line"; done <<< "$content"
  emit "\`\`\`"
}

# =======================================================================
# 0. Overview
# =======================================================================
h1 "Capsule Audit Summary"
kv "Config file"   "\`$AUDIT_CONFIG_PATH\`"
kv "Namespace"     "\`$NS\`"
kv "Run directory" "\`$DIR\`"
RUN_TS=$(stat -c %y "$DIR" 2>/dev/null | cut -d. -f1)
[[ -n "${RUN_TS:-}" ]] && kv "Captured at" "$RUN_TS"

# =======================================================================
# 1. Pod snapshot
# =======================================================================
h2 "1. Pod snapshot"
if [[ -s "$DIR/pods.txt" ]]; then
  pods_content=$(awk 'NR==1 || NF>0' "$DIR/pods.txt")
  code_block "$pods_content"
  # Restart anomaly scan
  restarts=$(awk 'NR>1 && $4+0>0 {print $1"("$4")"}' "$DIR/pods.txt" | paste -sd, -)
  if [[ -n "$restarts" ]]; then
    emit "$(yellow "Pods with restarts"): $restarts"
  else
    emit "$(green "All pods report 0 restarts.")"
  fi
else
  emit "$(yellow "No pods.txt — skipping.")"
fi

# =======================================================================
# 2. Stage 1 — Upload / etl-idea
# =======================================================================
h2 "2. Stage 1 — Upload / etl-idea"

ETL_LOG="$DIR/etl-idea.log"
if [[ -f "$ETL_LOG" ]]; then
  ETL_LC=$(lc "$ETL_LOG")
  kv "Log size" "$(fmt "$ETL_LC") lines"

  # Strict HTTP-error detection — match only real upload-error signatures.
  # Never match a bare `413` (too many false positives from row IDs / IP addresses).
  HTTP_ERR=$(grep -cE 'Payload Too Large|MaxUploadSizeExceededException|MultipartException|SizeLimitExceededException|FileSizeLimitExceededException' "$ETL_LOG" 2>/dev/null); HTTP_ERR=${HTTP_ERR:-0}
  kv "Real HTTP-level upload errors" "$HTTP_ERR"

  # Class-instantiation faults per resource
  emit
  emit "**Class-instantiation faults per resource (from log):**"
  faults=$(grep -oE 'Fault during class instantiation: [A-Za-z]+' "$ETL_LOG" \
           | awk '{print $NF}' | sort | uniq -c | sort -rn)
  if [[ -n "$faults" ]]; then
    code_block "$faults"
  else
    emit "$(dim "No class-instantiation faults logged.")"
  fi

  # Wrong number format
  WNF=$(grep -c 'Wrong number format' "$ETL_LOG" 2>/dev/null); WNF=${WNF:-0}
  if [[ "$WNF" -gt 0 ]]; then
    kv "Wrong-number-format lines" "$WNF"
    # Extract each offending row (ExcelRecord line immediately follows the error line)
    WNF_TSV="$DIR/wrong-number-format-rows.tsv"
    {
      printf "record_id\trow_id\tproperty_in_error\tdate\tbad_value\tparent_id\n"
      awk '
        /Wrong number format:/ { bad=1; next }
        bad && /\|____ExcelRecord\(/ {
          gsub(/.*\|____ExcelRecord\(/, "")
          gsub(/\)$/, "")
          n=split($0, a, ", ")
          printf "%s\t%s\t%s\t%s\t%s\t%s\n", a[1], a[2], a[4], a[5], a[6], a[7]
          bad=0
        }
      ' "$ETL_LOG"
    } > "$WNF_TSV"
    emit
    emit "**Rows with unparseable numbers** (also saved to \`wrong-number-format-rows.tsv\`):"
    # Pretty-print the TSV as a fixed-width block
    pretty=$(awk -F'\t' '{printf "%-10s %-10s %-32s %-12s %-12s %s\n", $1,$2,$3,$4,$5,$6}' "$WNF_TSV")
    code_block "$pretty"
    # Quick patterns
    DISTINCT_PROP=$(awk -F'\t' 'NR>1 {print $3}' "$WNF_TSV" | sort -u | wc -l)
    DISTINCT_VAL=$(awk -F'\t' 'NR>1 {print $5}' "$WNF_TSV" | sort -u | wc -l)
    kv "Distinct fields affected" "$DISTINCT_PROP"
    kv "Distinct bad values"      "$DISTINCT_VAL"
  fi
else
  emit "$(yellow "No etl-idea.log — skipping.")"
fi

# =======================================================================
# 3. Stage 2 — Staging (Aerospike + audit API)
# =======================================================================
h2 "3. Stage 2 — Staging (Aerospike + audit API)"

AS_EXCEL=$(num_from "$DIR/aerospike-sets.txt" "${AERO_SET_RECORDS}|^objects" 0)
# The pattern above picks the first `objects=` line which lives under
# the records-set section. Extract explicitly for safety:
if [[ -f "$DIR/aerospike-sets.txt" ]]; then
  AS_EXCEL=$(awk -v s="[${AERO_SET_RECORDS}]" '$0==s{flag=1;next} /^\[/{flag=0} flag && /^objects=/{sub(/^objects=/,"");print;exit}' "$DIR/aerospike-sets.txt")
  AS_ERR=$(awk   -v s="[${AERO_SET_ERRORS}]"  '$0==s{flag=1;next} /^\[/{flag=0} flag && /^objects=/{sub(/^objects=/,"");print;exit}' "$DIR/aerospike-sets.txt")
fi
AS_EXCEL=${AS_EXCEL:-0}
AS_ERR=${AS_ERR:-0}

# Audit API counts (should equal Aerospike counts)
API_RECORDS=$(json_len "$DIR/etl-records.json")
API_ERRORS=$(json_len "$DIR/etl-errors.json")

emit "| Source | Records | Errors |"
emit "|---|---:|---:|"
emit "| Aerospike | $(fmt "$AS_EXCEL") | $(fmt "$AS_ERR") |"
emit "| Audit API | $(fmt "$API_RECORDS") | $(fmt "$API_ERRORS") |"

# Reconcile
emit
if [[ "$AS_EXCEL" == "$API_RECORDS" && "$AS_ERR" == "$API_ERRORS" && "$AS_EXCEL" -gt 0 ]]; then
  emit "$(green "Aerospike and Audit API agree.") ✓"
elif [[ "$API_RECORDS" == "?" || "$API_ERRORS" == "?" ]]; then
  emit "$(yellow "jq not installed — API counts unavailable for reconciliation.")"
else
  emit "$(yellow "Mismatch between Aerospike and Audit API — investigate.")"
fi

# Staging table counts
emit
emit "**Staging table row counts (\`public.*\` in \`${ETL_DB_NAME}\`):**"
if [[ -s "$DIR/staging-counts.txt" ]]; then
  # Keep only the table|row_count block (strip psql borders).
  tab=$(sed -n '/^ *table_name/,/^\(([0-9]* rows)\)/p' "$DIR/staging-counts.txt" \
        | grep -Ev '^[-+]+$|^\(.*rows\)$|^ *$')
  code_block "$tab"
else
  emit "$(yellow "No staging-counts.txt — skipping.")"
fi

# ---------- Per-resource reconciliation: audit API vs staging table ----------
h3 "Per-resource reconciliation"

if [[ -s "$DIR/etl-records-by-resource.txt" && -s "$DIR/staging-counts.txt" ]]; then
  emit "_For each resource the audit API saw:_"
  emit "- \`records\` = distinct resource **instances** the pipeline processed (grouped by \`recordId\` + \`coreVariable\` prefix in \`etl-records.json\`)."
  emit "- \`errored\` = distinct resource instances with **at least one** field-level error (from \`etl-errors.json\`, grouped by \`recordId\` per \`resourceName\`)."
  emit "- \`staging\` = row count in the corresponding staging table."
  emit "- \`missing\` = \`records − staging\` — rows the audit API saw but the staging table doesn't have."
  emit "- \`status\` — see the table legend below."
  emit
  emit "_Resource → table mapping is CamelCase → snake_case by default (e.g. \`SystemicTreatment\` → \`systemic_treatment\`). Override via \`RESOURCE_TO_TABLE_OVERRIDE\` in the config for names that don't follow that rule._"
  emit

  recon=$(awk -v overrides="$RESOURCE_TO_TABLE_OVERRIDE" '
    function camel_to_snake(s,   r, c, i) {
      r = ""
      for (i = 1; i <= length(s); i++) {
        c = substr(s, i, 1)
        if (c ~ /[A-Z]/ && i > 1) r = r "_"
        r = r tolower(c)
      }
      return r
    }
    BEGIN {
      n = split(overrides, pairs, /[ \t]+/)
      for (i = 1; i <= n; i++) {
        if (pairs[i] == "") continue
        eq = index(pairs[i], ":")
        if (eq > 0) override[substr(pairs[i], 1, eq-1)] = substr(pairs[i], eq+1)
      }
      file = 0
    }
    FNR == 1 { file++ }

    # File 1: etl-records-by-resource.txt  ("    1234 ResourceName")
    #   (distinct recordId per resource; produced by the collector)
    file == 1 {
      cnt = $1; res = $2
      if (res == "" || res == "(none)" || cnt !~ /^[0-9]+$/) next
      records[res] = cnt
      if (!(res in seen)) { seen[res] = 1; order[++K] = res }
      next
    }

    # File 2: etl-errors-by-resource-instances.txt (same format)
    #   (distinct recordId per resourceName in errors.json)
    file == 2 {
      cnt = $1; res = $2
      if (res == "" || res == "(none)" || cnt !~ /^[0-9]+$/) next
      errored[res] = cnt
      if (!(res in seen)) { seen[res] = 1; order[++K] = res }
      next
    }

    # File 3: staging-counts.txt (psql output: "  systemic_treatment | 1205")
    file == 3 {
      if ($0 !~ /\|/ || $0 ~ /^[-+ ]+$/) next
      split($0, b, /\|/)
      tbl = b[1]; cnt = b[2]
      gsub(/^[ \t]+|[ \t]+$/, "", tbl)
      gsub(/^[ \t]+|[ \t]+$/, "", cnt)
      if (tbl == "" || tbl == "table_name" || cnt !~ /^[0-9]+$/) next
      staging[tbl] = cnt
      next
    }

    END {
      # Sort resources by records desc (biggest on top)
      for (i = 1; i <= K; i++) {
        res = order[i]
        r = (res in records) ? records[res] : 0
        j = i
        while (j > 1 && ((order[j-1] in records ? records[order[j-1]] : 0) < r)) {
          tmp = order[j]; order[j] = order[j-1]; order[j-1] = tmp
          j--
        }
      }

      printf "%-30s %10s %10s %10s %10s %-14s %s\n", \
        "resource", "records", "errored", "staging", "missing", "status", "table"

      for (i = 1; i <= K; i++) {
        res = order[i]
        r = (res in records)  ? records[res]+0  : 0
        e = (res in errored)  ? errored[res]+0  : 0
        tbl = (res in override) ? override[res] : camel_to_snake(res)
        if (tbl in staging) {
          s = staging[tbl]+0
          m = r - s
          if      (m == 0)                 status = "clean"
          else if (m < 0)                  status = "DUPLICATES"
          else if (m <= e)                 status = "explained"
          else                             status = "SILENT_LOSS"
          printf "%-30s %10d %10d %10d %+10d %-14s %s\n", res, r, e, s, m, status, tbl
        } else {
          printf "%-30s %10d %10d %10s %10s %-14s %s\n", res, r, e, "-", "-", "no_table", tbl
        }
      }
    }
  ' "$DIR/etl-records-by-resource.txt" \
    "$DIR/etl-errors-by-resource-instances.txt" \
    "$DIR/staging-counts.txt")

  code_block "$recon"

  emit "_**Status legend**: \`clean\` = every instance landed in staging; \`explained\` = some didn't, but at least as many instances had errors (likely blocked by those errors — this is the normal case for resources with validation failures); \`SILENT_LOSS\` = more rows are missing than had errors (rows vanished without a corresponding error — investigate); \`DUPLICATES\` = more rows in staging than records saw (duplicate inserts or leftover data); \`no_table\` = no staging table matched (set \`RESOURCE_TO_TABLE_OVERRIDE\` or confirm none exists)._"
  emit

  # Collect the concerning rows
  silent=$(echo "$recon" | awk '$6=="SILENT_LOSS" {printf "%s(%d missing, only %d errored) ", $1, $5+0, $3+0}')
  dup=$(echo    "$recon" | awk '$6=="DUPLICATES"  {printf "%s(%+d) ", $1, $5+0}')
  nomatch=$(echo "$recon" | awk '$6=="no_table"   {print $1}' | paste -sd, -)
  exp=$(echo    "$recon" | awk '$6=="explained"   {count++} END{print count+0}')

  if [[ -z "$silent" && -z "$dup" && -z "$nomatch" ]]; then
    if [[ "$exp" -gt 0 ]]; then
      emit "$(green "No silent data loss.") ✓ ($exp resources lost rows but every loss is explained by errors.)"
    else
      emit "$(green "All resources reconcile perfectly.") ✓"
    fi
  else
    [[ -n "$silent"  ]] && emit "$(red    "SILENT DATA LOSS"): ${silent% }"
    [[ -n "$dup"     ]] && emit "$(yellow "Duplicates in staging"): ${dup% }"
    [[ -n "$nomatch" ]] && emit "$(yellow "No matching staging table"): $nomatch — set \`RESOURCE_TO_TABLE_OVERRIDE\` or confirm no dedicated table."
  fi
else
  if [[ ! -s "$DIR/etl-records-by-resource.txt" ]]; then
    emit "$(yellow "No etl-records-by-resource.txt (old artifact bundle or jq unavailable during collection) — skipping per-resource reconciliation.")"
  else
    emit "$(yellow "No staging-counts.txt — skipping per-resource reconciliation.")"
  fi
fi

# Error classification from API breakdowns
if [[ -f "$DIR/etl-errors-by-code.txt" || -f "$DIR/etl-errors-by-resource.txt" ]]; then
  h3 "Error classification"

  if [[ -s "$DIR/etl-errors-by-code.txt" ]]; then
    emit "**By \`error\` code:**"
    code_block "$(cat "$DIR/etl-errors-by-code.txt")"
  fi

  if [[ -s "$DIR/etl-errors-by-resource.txt" ]]; then
    emit "**By \`resourceName\` (CSV section):**"
    code_block "$(head -20 "$DIR/etl-errors-by-resource.txt")"
  fi

  if [[ -s "$DIR/etl-errors-by-property.txt" ]]; then
    emit "**By \`propertyInError\` (which field failed):**"
    code_block "$(head -20 "$DIR/etl-errors-by-property.txt")"
    # Highlight: how many errors carry a specific field?
    specific=$(awk '$2!="(none)" && NF>=2 {s+=$1} END{print s+0}' "$DIR/etl-errors-by-property.txt")
    none=$(awk '$2=="(none)" {s+=$1} END{print s+0}' "$DIR/etl-errors-by-property.txt")
    total=$((specific+none))
    if [[ $total -gt 0 ]]; then
      pct=$((100*specific/total))
      emit "_Only **${specific}** of ${total} errors (${pct}%) identify a specific field; the rest are class-level failures._"
    fi
  fi

  if [[ -s "$DIR/etl-errors-by-motivation.txt" ]]; then
    emit "**Top \`motivation\` prefixes:**"
    code_block "$(head -10 "$DIR/etl-errors-by-motivation.txt")"
  fi

  if [[ -s "$DIR/etl-errors-samples.tsv" ]]; then
    emit "**Sample rows:**"
    code_block "$(head -5 "$DIR/etl-errors-samples.tsv")"
  fi
fi

# =======================================================================
# 4. Stage 3 — OMOP ETL
# =======================================================================
h2 "4. Stage 3 — OMOP ETL"

OMOP_LOG="$DIR/omop-etl.log"
if [[ -f "$OMOP_LOG" ]]; then
  # Job status
  JOB_STATUS=$(grep -oE 'status: \[[A-Z]+\]|completed with the following parameters.*status: \[[A-Z]+\]' "$OMOP_LOG" \
               | tail -1 | grep -oE '\[[A-Z]+\]' | tr -d '[]')
  JOB_DURATION=$(grep -oE 'in [0-9hms]+ms?' "$OMOP_LOG" | tail -1)
  [[ -z "$JOB_STATUS" ]] && JOB_STATUS=$(grep -oE 'BatchStatus=[A-Z]+' "$OMOP_LOG" | tail -1 | cut -d= -f2)
  [[ -z "$JOB_STATUS" ]] && JOB_STATUS="(unknown)"

  if [[ "$JOB_STATUS" == "COMPLETED" ]]; then
    kv "Job status" "$(green "$JOB_STATUS")"
  elif [[ "$JOB_STATUS" == "FAILED" ]]; then
    kv "Job status" "$(red "$JOB_STATUS")"
  else
    kv "Job status" "$JOB_STATUS"
  fi
  [[ -n "$JOB_DURATION" ]] && kv "Job duration" "${JOB_DURATION# in }"
else
  emit "$(yellow "No omop-etl.log — skipping job status.")"
fi

# Severe errors
SEVERE_LINES=$(lc "$DIR/omop-severe.txt")
if [[ "$SEVERE_LINES" -eq 0 ]]; then
  kv "Severe (non-skip) errors" "$(green "0") ✓"
else
  kv "Severe (non-skip) errors" "$(red "$SEVERE_LINES")"
  emit "First 10 lines of \`omop-severe.txt\`:"
  code_block "$(head -10 "$DIR/omop-severe.txt")"
fi

# Skips
if [[ -s "$DIR/omop-skip-by-type.txt" ]]; then
  emit
  emit "**EXCEPTION SKIP — by type + step:**"
  code_block "$(cat "$DIR/omop-skip-by-type.txt")"

  TOTAL_SKIP=$(awk '{s+=$1} END{print s+0}' "$DIR/omop-skip-by-type.txt")
  RETRY_SKIP=$(awk '/DuplicateKeyException/ {s+=$1} END{print s+0}' "$DIR/omop-skip-by-type.txt")
  REAL_SKIP=$((TOTAL_SKIP-RETRY_SKIP))
  kv "Total skips" "$TOTAL_SKIP"
  kv "├─ retry artifacts (DuplicateKeyException)" "$RETRY_SKIP (not real data loss)"
  kv "└─ real losses (other exceptions)" "$REAL_SKIP"
fi

if [[ -s "$DIR/omop-skip-ids.txt" ]]; then
  emit "**Distinct skipped source IDs:** $(lc "$DIR/omop-skip-ids.txt")"
fi

# Step counts summary
if [[ -s "$DIR/omop-step-counts.txt" ]]; then
  h3 "STEP_COUNT highlights"

  # Totals
  TOT_READ=$(awk 'NR>1 {s+=$2} END{print s+0}' "$DIR/omop-step-counts.txt")
  TOT_WRITE=$(awk 'NR>1 {s+=$3} END{print s+0}' "$DIR/omop-step-counts.txt")
  TOT_FILTER=$(awk 'NR>1 {s+=$4} END{print s+0}' "$DIR/omop-step-counts.txt")
  TOT_SKIP=$(awk 'NR>1 {s+=$5} END{print s+0}' "$DIR/omop-step-counts.txt")
  emit "Totals across all steps: read=$(fmt "$TOT_READ")  write=$(fmt "$TOT_WRITE")  filter=$(fmt "$TOT_FILTER")  skip=$(fmt "$TOT_SKIP")"
  emit

  emit "**Steps that filtered 100% (read>0, write=0) — mapping gaps:**"
  emit "_When a step filters 100%, \`read\` and \`filter\` are equal by definition (every row read was filtered)._"
  filt_hdr=$(printf "%-45s %8s %8s\n" "step" "read" "filter")
  filt_body=$(awk 'NR>1 && $2>0 && $3==0 {printf "%-45s %8s %8s\n", $1, $2, $4}' "$DIR/omop-step-counts.txt")
  code_block "${filt_hdr}
${filt_body:-<none>}"

  emit "**Steps with partial filter (write>0 and filter>0) — partial mapping gaps:**"
  emit "_Some rows were mapped, some were intentionally dropped. Investigate only if the filtered share is higher than expected._"
  pfilt_hdr=$(printf "%-45s %8s %8s %8s %6s\n" "step" "read" "write" "filter" "filt%")
  pfilt_body=$(awk 'NR>1 && $3>0 && $4>0 {
    pct = ($2>0) ? (100*$4/$2) : 0
    printf "%-45s %8s %8s %8s %5.1f%%\n", $1, $2, $3, $4, pct
  }' "$DIR/omop-step-counts.txt")
  code_block "${pfilt_hdr}
${pfilt_body:-<none>}"

  emit "**Steps with any skip>0:**"
  sk=$(awk 'NR>1 && $5>0 {printf "%-45s read=%s write=%s skip=%s\n", $1, $2, $3, $5}' "$DIR/omop-step-counts.txt")
  code_block "${sk:-<none>}"

  emit "**Steps with 0 reads (section absent from CSV):**"
  zr=$(awk 'NR>1 && $2==0 {print $1}' "$DIR/omop-step-counts.txt")
  code_block "${zr:-<none>}"
fi

# =======================================================================
# 5. Referential errors across layers
# =======================================================================
h2 "5. Referential errors — the same root cause in four layers"

emit "Broken/missing references (a child row pointing at a parent that doesn't exist) surface at four layers of the pipeline. A **single bad CSV reference can fan out into all four places**, so these counts are **not independent** — they measure the same pathology at different grains."

# --- Layer A: Stage 1 per-row RECORD_CONVERSION_ERROR ---
LAYER_A=0
if [[ -s "$DIR/etl-errors-by-code.txt" ]]; then
  LAYER_A=$(awk '/RECORD_CONVERSION_ERROR/ {print $1; exit}' "$DIR/etl-errors-by-code.txt")
  LAYER_A=${LAYER_A:-0}
fi

# --- Layer B: Stage 1 global RESOURCE_LINKAGE_ERROR + fan-out count from log ---
LAYER_B=0
BLOCKED_ROWS=0
if [[ -s "$DIR/etl-errors-by-code.txt" ]]; then
  LAYER_B=$(awk '/RESOURCE_LINKAGE_ERROR/ {print $1; exit}' "$DIR/etl-errors-by-code.txt")
  LAYER_B=${LAYER_B:-0}
fi
if [[ -f "$ETL_LOG" ]]; then
  BLOCKED_ROWS=$(grep -oE 'Unsaved resources:[[:space:]]*[0-9]+' "$ETL_LOG" | tail -1 | grep -oE '[0-9]+' || echo 0)
  BLOCKED_ROWS=${BLOCKED_ROWS:-0}
fi

# --- Layer C: Stage 3 filters (intentional) — already in TOT_FILTER ---
LAYER_C=${TOT_FILTER:-0}

# --- Layer D: Stage 3 real skips (non-retry) — already in REAL_SKIP ---
LAYER_D=${REAL_SKIP:-0}

# Render the table
emit
emit "| Layer | Where it fires | Error type | Grain | Count |"
emit "|---|---|---|---|---:|"
emit "| A | Stage 1 — \`etl-idea\`, per-row validator | \`RECORD_CONVERSION_ERROR\` | 1 bad row | $(fmt "$LAYER_A") |"
if [[ "$LAYER_B" -gt 0 && "$BLOCKED_ROWS" -gt 0 ]]; then
  emit "| B | Stage 1 — \`etl-idea\`, global linkage pass | \`RESOURCE_LINKAGE_ERROR\` | 1 batch summary = many rows | $(fmt "$LAYER_B") (→ $(fmt "$BLOCKED_ROWS") blocked rows) |"
else
  emit "| B | Stage 1 — \`etl-idea\`, global linkage pass | \`RESOURCE_LINKAGE_ERROR\` | 1 batch summary = many rows | $(fmt "$LAYER_B") |"
fi
emit "| C | Stage 3 — \`omop-etl\`, Spring Batch processor | filter (intentional, **not an error**) | 1 row that has nothing to become in OMOP | $(fmt "$LAYER_C") |"
emit "| D | Stage 3 — \`omop-etl\`, skip-on-error | \`NullPointerException\` / other | 1 row whose parent ref was null | $(fmt "$LAYER_D") |"

emit
emit "**How to read these layers:**"
emit "- **A** = \"this row is malformed\" (e.g. \`SystemicTreatment.diagnosisReference\` is null or unparseable)."
emit "- **B** = \"this row is structurally fine, but its parent was killed by A, so I refuse to persist the orphan.\" The audit API shows **1** entry; the log's \`Blocked record_id list\` expands it into the full row count."
emit "- **C** = Spring Batch saw a staging row that passed A+B but had nothing to map to in OMOP (no concept, no parent episode). Row silently drops. **Expected** for sections OMOP doesn't model; **investigate** only if a domain you care about is 100% filtered."
emit "- **D** = Spring Batch processor dereferenced a field that should have been there, hit \`null\`, skipped the chunk. Usually a second-order effect of A/B."
emit
emit "**Investigation order:** start at **A** (\`etl-errors-by-resource.txt\`, \`etl-errors-samples.tsv\`) → then **B** (\`grep 'Blocked record_id list' $DIR/etl-idea.log\`) → only then **C/D** (\`omop-step-counts.txt\`, \`omop-skip-by-type.txt\`). Fixing A usually makes B, C, and D shrink automatically."

# =======================================================================
# 6. Handshake (staging vs OMOP)
# =======================================================================
h2 "6. Handshake — staging vs OMOP"

STAGING_PATIENTS=""
OMOP_PERSONS=""
if [[ -f "$DIR/staging-counts.txt" ]]; then
  # Derive the short table name (last segment of schema.table) for the grep.
  PATIENT_SHORT="${ETL_DB_PATIENT_TABLE##*.}"
  STAGING_PATIENTS=$(awk -v t="$PATIENT_SHORT" '$1==t {print $3; exit}' "$DIR/staging-counts.txt")
fi
if [[ -f "$DIR/omop-counts.txt" ]]; then
  # The omop-counts.txt format is a psql table — extract the 'person' column (1st data cell).
  OMOP_PERSONS=$(awk '
    /\|/ && /visit_occurrence/ {header=NR; next}
    header && NR==header+2 {
      gsub(/[ \t]/, "", $1); split($0,a,"|");
      # first field might be empty; take the first non-empty value
      for(i=1;i<=length(a);i++){v=a[i]; gsub(/[ \t]/,"",v); if(v ~ /^[0-9]+$/){print v; exit}}
    }' "$DIR/omop-counts.txt")
fi

kv "Staging patients" "${STAGING_PATIENTS:-?}"
kv "OMOP persons"     "${OMOP_PERSONS:-?}"

if [[ -n "$STAGING_PATIENTS" && -n "$OMOP_PERSONS" && "$STAGING_PATIENTS" == "$OMOP_PERSONS" ]]; then
  emit "$(green "MATCH") ✓"
elif [[ -n "$STAGING_PATIENTS" && -n "$OMOP_PERSONS" ]]; then
  emit "$(red "MISMATCH") ✗ — investigate skipped rows in Stage 3"
else
  emit "$(yellow "Insufficient data to compute handshake.")"
fi

# OMOP domain counts
if [[ -s "$DIR/omop-counts.txt" ]]; then
  emit
  emit "**OMOP domain counts:**"
  oc=$(sed -n '/|/p' "$DIR/omop-counts.txt" | head -n 4)
  code_block "$oc"
fi

# =======================================================================
# 7. Verdict
# =======================================================================
h2 "7. Verdict"

FAIL=0
WARN=0
VERDICT=""

# Real HTTP errors
if [[ "${HTTP_ERR:-0}" -gt 0 ]]; then
  VERDICT+="- ${HTTP_ERR} real HTTP upload errors — check \`etl-idea.log\`.\n"
  FAIL=1
fi

# Severe OMOP
if [[ "${SEVERE_LINES:-0}" -gt 0 ]]; then
  VERDICT+="- ${SEVERE_LINES} severe OMOP errors — check \`omop-severe.txt\`.\n"
  FAIL=1
fi

# Staging ↔ OMOP mismatch
if [[ -n "${STAGING_PATIENTS:-}" && -n "${OMOP_PERSONS:-}" && "${STAGING_PATIENTS}" != "${OMOP_PERSONS}" ]]; then
  VERDICT+="- Handshake MISMATCH: staging=${STAGING_PATIENTS}, OMOP=${OMOP_PERSONS}.\n"
  FAIL=1
fi

# Drug exposure zero
if grep -q 'drugsToDrugExposureStep.*write=0\|drugsToDrugExposureStep *[0-9]* *0 ' "$DIR/omop-step-counts.txt" 2>/dev/null; then
  VERDICT+="- \`drug_exposure = 0\` — drug mapping gap (investigate \`drugsToDrugExposureStep\`).\n"
  WARN=1
fi

# Large ETL error count
if [[ "${API_ERRORS:-0}" =~ ^[0-9]+$ && "${API_ERRORS}" -gt 1000 ]]; then
  VERDICT+="- Large number of staging errors ($(fmt "$API_ERRORS")) — see \`etl-errors-by-resource.txt\`.\n"
  WARN=1
fi

# Real skip losses
if [[ "${REAL_SKIP:-0}" -gt 0 ]]; then
  VERDICT+="- ${REAL_SKIP} real OMOP skip losses (non-retry) — see \`omop-skip-ids.txt\`.\n"
  WARN=1
fi

# Per-resource reconciliation: silent data loss is a hard fail
if [[ -n "${silent:-}" ]]; then
  VERDICT+="- Silent data loss (rows missing without corresponding errors): ${silent% } — investigate \`etl-records-by-resource.txt\`, \`etl-errors-by-resource-instances.txt\`, and \`staging-counts.txt\`.\n"
  FAIL=1
fi

# Per-resource reconciliation: duplicates in staging is a warning (usually
# leftover data from a previous run or a retry artifact)
if [[ -n "${dup:-}" ]]; then
  VERDICT+="- Duplicates in staging: ${dup% } — more rows in staging tables than the audit API reports. Usually leftover from a prior run; confirm staging was truncated before this upload.\n"
  WARN=1
fi

if [[ $FAIL -eq 1 ]]; then
  emit "$(red "Pipeline health: DEGRADED — follow-up required.")"
elif [[ $WARN -eq 1 ]]; then
  emit "$(yellow "Pipeline health: OK with caveats.")"
else
  emit "$(green "Pipeline health: HEALTHY ✓")"
fi

if [[ -n "$VERDICT" ]]; then
  emit
  emit "Follow-ups:"
  # Process substitution instead of a pipe — keeps the loop in the
  # parent shell so emit() can update the markdown buffer (`_plain`).
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    emit "$line"
  done < <(echo -e "$VERDICT")
fi

# =======================================================================
# Write markdown copy if requested
# =======================================================================
if [[ -n "$MD_OUT" ]]; then
  mkdir -p "$(dirname "$MD_OUT")"
  printf "%s" "$_plain" > "$MD_OUT"
  echo
  echo "Markdown summary written to: $MD_OUT"
fi
