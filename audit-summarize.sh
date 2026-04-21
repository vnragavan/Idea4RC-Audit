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
      printf "record_id\trow_id\tproperty_in_error\tbad_value\tparent_id\n"
      awk '
        /Wrong number format:/ { bad=1; next }
        bad && /\|____ExcelRecord\(/ {
          gsub(/.*\|____ExcelRecord\(/, "")
          gsub(/\)$/, "")
          n=split($0, a, ", ")
          printf "%s\t%s\t%s\t%s\t%s\n", a[1], a[2], a[4], a[6], a[7]
          bad=0
        }
      ' "$ETL_LOG"
    } > "$WNF_TSV"
    emit
    emit "**Rows with unparseable numbers** (also saved to \`wrong-number-format-rows.tsv\`):"
    # Pretty-print the TSV as a fixed-width block
    pretty=$(awk -F'\t' '{printf "%-10s %-10s %-32s %-12s %s\n", $1,$2,$3,$4,$5}' "$WNF_TSV")
    code_block "$pretty"
    # Quick patterns
    DISTINCT_PROP=$(awk -F'\t' 'NR>1 {print $3}' "$WNF_TSV" | sort -u | wc -l)
    DISTINCT_VAL=$(awk -F'\t' 'NR>1 {print $4}' "$WNF_TSV" | sort -u | wc -l)
    kv "Distinct fields affected" "$DISTINCT_PROP"
    kv "Distinct bad values"      "$DISTINCT_VAL"
  fi

  # -----------------------------------------------------------------
  # Global linkage abort (log-only — the audit API just flags "see
  # logs", so this block is the only place the scale of the abort
  # surfaces in Stage 1).
  # -----------------------------------------------------------------
  UNSAVED=$(grep -oE 'Unsaved resources: [0-9]+' "$ETL_LOG" | awk '{print $NF}' | tail -1)
  if [[ -n "$UNSAVED" ]]; then
    emit
    emit "**Global linkage abort (log-only):**"
    kv "Blocked rows (unresolved dependencies)" "$(fmt "$UNSAVED")"
    BLOCKED_IDS_FILE="$DIR/blocked-record-ids.txt"
    grep -oE 'Blocked record_id list \(all\): \[[^]]*\]' "$ETL_LOG" \
      | sed -E 's/^.*\[//; s/\]$//; s/, /\n/g' \
      | awk 'NF' > "$BLOCKED_IDS_FILE"
    BLOCKED_COUNT=$(wc -l < "$BLOCKED_IDS_FILE" | tr -d ' ')
    kv "Blocked record_ids extracted" "$(fmt "$BLOCKED_COUNT") (saved to \`blocked-record-ids.txt\`)"
    examples_fmt=$(grep 'Examples of blocked resources' "$ETL_LOG" \
                   | head -1 \
                   | sed -E 's/^.*Examples of blocked resources \(first [0-9]+\): \[//; s/\]$//; s/, recordId=/\nrecordId=/g')
    if [[ -n "$examples_fmt" ]]; then
      emit "_Sample blocked resources:_"
      code_block "$examples_fmt"
    fi
  fi

  # -----------------------------------------------------------------
  # ERROR-line reconciliation — safety net so any new ERROR shape
  # (not matched by the patterns above) surfaces instead of being
  # silently ignored.
  # -----------------------------------------------------------------
  emit
  emit "**ERROR-line reconciliation:**"
  TOT_ERR=$(grep -cE ' ERROR [0-9]+ ---' "$ETL_LOG" 2>/dev/null); TOT_ERR=${TOT_ERR:-0}
  FAULT_TOT=$(grep -cE 'Fault during class instantiation' "$ETL_LOG" 2>/dev/null); FAULT_TOT=${FAULT_TOT:-0}
  LINKAGE_TOT=$(grep -cE 'Unsaved resources:|Examples of blocked resources|Blocked record_id list' "$ETL_LOG" 2>/dev/null); LINKAGE_TOT=${LINKAGE_TOT:-0}
  HTTP_TOT=$(grep -cE 'Payload Too Large|MaxUploadSizeExceededException|MultipartException|SizeLimitExceededException|FileSizeLimitExceededException' "$ETL_LOG" 2>/dev/null); HTTP_TOT=${HTTP_TOT:-0}
  KNOWN=$((FAULT_TOT + WNF + LINKAGE_TOT + HTTP_TOT))
  UNKNOWN=$((TOT_ERR - KNOWN))
  [[ "$UNKNOWN" -lt 0 ]] && UNKNOWN=0
  kv "Total ERROR lines"                 "$(fmt "$TOT_ERR")"
  kv "Explained (faults+WNF+linkage+http)" "$(fmt "$KNOWN") ($(fmt "$FAULT_TOT") + $(fmt "$WNF") + $(fmt "$LINKAGE_TOT") + $(fmt "$HTTP_TOT"))"
  if [[ "$UNKNOWN" -gt 0 ]]; then
    emit "- $(yellow "Unexplained ERROR lines"): $(fmt "$UNKNOWN") (sample below — add a pattern to the summarizer)"
    sample=$(grep -E ' ERROR [0-9]+ ---' "$ETL_LOG" \
             | grep -vE 'Fault during class instantiation|Wrong number format|Unsaved resources:|Examples of blocked resources|Blocked record_id list|Payload Too Large|MaxUploadSizeExceededException|MultipartException|SizeLimitExceededException|FileSizeLimitExceededException' \
             | head -5 | cut -c1-240)
    [[ -n "$sample" ]] && code_block "$sample"
  else
    emit "- $(green "all ERROR lines explained.") ✓"
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
OMOP_DOMAINS_KV=""
if [[ -f "$DIR/omop-counts.txt" ]]; then
  # The omop-counts.txt format is a psql table — extract the 'person' column (1st data cell).
  OMOP_PERSONS=$(awk '
    /\|/ && /visit_occurrence/ {header=NR; next}
    header && NR==header+2 {
      gsub(/[ \t]/, "", $1); split($0,a,"|");
      # first field might be empty; take the first non-empty value
      for(i=1;i<=length(a);i++){v=a[i]; gsub(/[ \t]/,"",v); if(v ~ /^[0-9]+$/){print v; exit}}
    }' "$DIR/omop-counts.txt")

  # Full (domain → count) mapping — used by S7.
  OMOP_DOMAINS_KV=$(awk '
    /\|/ && /person/ && /visit_occurrence/ {
      split($0, hdr, /\|/)
      getline sep   # the separator line "---+---+..."
      getline row
      split(row, val, /\|/)
      for (i = 1; i <= length(hdr); i++) {
        h = hdr[i]; v = val[i]
        gsub(/^[ \t]+|[ \t]+$/, "", h)
        gsub(/^[ \t]+|[ \t]+$/, "", v)
        if (h != "" && v ~ /^[0-9]+$/) printf "%s=%s\n", h, v
      }
      exit
    }
  ' "$DIR/omop-counts.txt")
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
# 7. Success criteria — deterministic, config-driven verdict
# =======================================================================
h2 "7. Success criteria"

# Budgets & core lists come from audit-config-lib.sh (with audit.conf /
# env overrides). See `SUCCESS_BUDGET_*` and `SUCCESS_CORE_*`.

# ---------- Criteria plumbing ----------
# Worst tier seen: 0 = CLEAN, 1 = DEGRADED, 2 = FAILED.
WORST_TIER=0
CRIT_ROWS_MD=""       # markdown table body (for SUMMARY.md)
FAIL_REASONS=""       # human-readable reasons (for the Follow-ups block)

# crit CODE TIER PASS DESC RULE MEASURED
#   CODE     — stable identifier, e.g. I1, S2, B3
#   TIER     — FAILED | DEGRADED
#   PASS     — pass | fail | skip
#   DESC     — short description
#   RULE     — budget / invariant expressed in terms the reader can verify
#   MEASURED — the value we computed from the bundle
crit() {
  local code="$1" tier="$2" pass="$3" desc="$4" rule="$5" measured="$6"
  local mark
  case "$pass" in
    pass) mark="✓" ;;
    skip) mark="n/a" ;;
    fail)
      mark="✗"
      case "$tier" in
        FAILED)   [[ $WORST_TIER -lt 2 ]] && WORST_TIER=2 ;;
        DEGRADED) [[ $WORST_TIER -lt 1 ]] && WORST_TIER=1 ;;
      esac
      FAIL_REASONS+="- **${code}** ${desc}: ${measured} (rule: ${rule})"$'\n'
      ;;
  esac
  CRIT_ROWS_MD+="| ${code} | ${desc} | ${rule} | ${measured} | ${mark} |"$'\n'
}

# Helper: compare two numeric strings using awk (portable, handles floats).
# Returns 0 if $1 <= $2, else 1.
le_num() { awk -v a="$1" -v b="$2" 'BEGIN { exit (a+0 <= b+0) ? 0 : 1 }'; }

# Helper: pretty-print a float as 3-decimal percent. 0.00123 -> "0.123%"
pct3() { awk -v v="$1" 'BEGIN { printf "%.3f%%", (v+0)*100 }'; }

# ---------- I1. OMOP job status ----------
if [[ -n "${JOB_STATUS:-}" && "$JOB_STATUS" != "(unknown)" ]]; then
  if [[ "$JOB_STATUS" == "COMPLETED" ]]; then
    crit I1 FAILED pass "OMOP job status" "== COMPLETED" "$JOB_STATUS"
  else
    crit I1 FAILED fail "OMOP job status" "== COMPLETED" "$JOB_STATUS"
  fi
else
  crit I1 FAILED skip "OMOP job status" "== COMPLETED" "no omop-etl.log"
fi

# ---------- I2. Aerospike ↔ API records ----------
if [[ "${AS_EXCEL:-?}" =~ ^[0-9]+$ && "${API_RECORDS:-?}" =~ ^[0-9]+$ ]]; then
  if [[ "$AS_EXCEL" == "$API_RECORDS" ]]; then
    crit I2 FAILED pass "Aerospike ↔ API records" "equal" "$(fmt "$AS_EXCEL") / $(fmt "$API_RECORDS")"
  else
    crit I2 FAILED fail "Aerospike ↔ API records" "equal" "$(fmt "$AS_EXCEL") / $(fmt "$API_RECORDS")"
  fi
else
  crit I2 FAILED skip "Aerospike ↔ API records" "equal" "missing aerospike-sets.txt or etl-records.json"
fi

# ---------- I3. Aerospike ↔ API errors ----------
if [[ "${AS_ERR:-?}" =~ ^[0-9]+$ && "${API_ERRORS:-?}" =~ ^[0-9]+$ ]]; then
  if [[ "$AS_ERR" == "$API_ERRORS" ]]; then
    crit I3 FAILED pass "Aerospike ↔ API errors" "equal" "$(fmt "$AS_ERR") / $(fmt "$API_ERRORS")"
  else
    crit I3 FAILED fail "Aerospike ↔ API errors" "equal" "$(fmt "$AS_ERR") / $(fmt "$API_ERRORS")"
  fi
else
  crit I3 FAILED skip "Aerospike ↔ API errors" "equal" "missing aerospike-sets.txt or etl-errors.json"
fi

# ---------- I4. Staging patients ↔ OMOP persons ----------
if [[ "${STAGING_PATIENTS:-?}" =~ ^[0-9]+$ && "${OMOP_PERSONS:-?}" =~ ^[0-9]+$ ]]; then
  if [[ "$STAGING_PATIENTS" == "$OMOP_PERSONS" ]]; then
    crit I4 FAILED pass "staging_patients ↔ omop_persons" "equal" "$(fmt "$STAGING_PATIENTS") / $(fmt "$OMOP_PERSONS")"
  else
    crit I4 FAILED fail "staging_patients ↔ omop_persons" "equal" "$(fmt "$STAGING_PATIENTS") / $(fmt "$OMOP_PERSONS")"
  fi
else
  crit I4 FAILED skip "staging_patients ↔ omop_persons" "equal" "missing staging-counts.txt or omop-counts.txt"
fi

# ---------- I5. OMOP step accounting (read == write + filter + skip) ----------
# Treat "only the header row was parsed" as a skip, not a pass — otherwise
# a drift in the STEP_COUNT log format (e.g. reordered fields) would leave
# the file empty and let I5 pass vacuously while Stage 3 is actually broken.
if [[ -s "$DIR/omop-step-counts.txt" ]]; then
  step_rows=$(awk 'NR>1 && NF>=5' "$DIR/omop-step-counts.txt" | wc -l | tr -d ' ')
  if [[ "$step_rows" -eq 0 ]]; then
    crit I5 FAILED skip "Step accounting (read == write+filter+skip)" "all steps" "no data rows — check STEP_COUNT log format"
  else
    bad_steps=$(awk 'NR>1 && ($2+0) != ($3+0 + $4+0 + $5+0) {
      printf "%s(read=%s,w=%s,f=%s,s=%s) ", $1, $2, $3, $4, $5
    }' "$DIR/omop-step-counts.txt")
    if [[ -z "$bad_steps" ]]; then
      crit I5 FAILED pass "Step accounting (read == write+filter+skip)" "all steps" "ok ($(fmt "$step_rows") steps)"
    else
      crit I5 FAILED fail "Step accounting (read == write+filter+skip)" "all steps" "${bad_steps% }"
    fi
  fi
else
  crit I5 FAILED skip "Step accounting (read == write+filter+skip)" "all steps" "missing omop-step-counts.txt"
fi

# ---------- I6. Stage 1 log ERROR reconciliation (remainder == 0) ----------
if [[ -n "${TOT_ERR:-}" ]]; then
  if [[ "${UNKNOWN:-0}" -eq 0 ]]; then
    crit I6 FAILED pass "Stage 1 ERROR reconciliation" "unexplained == 0" "$(fmt "${UNKNOWN:-0}") of $(fmt "$TOT_ERR")"
  else
    crit I6 FAILED fail "Stage 1 ERROR reconciliation" "unexplained == 0" "$(fmt "$UNKNOWN") of $(fmt "$TOT_ERR") unexplained"
  fi
else
  crit I6 FAILED skip "Stage 1 ERROR reconciliation" "unexplained == 0" "no etl-idea.log"
fi

# ---------- I7. Allowed API error codes ----------
if [[ -s "$DIR/etl-errors-by-code.txt" ]]; then
  unknown_codes=$(awk '$2 != "" && $2 != "RECORD_CONVERSION_ERROR" && $2 != "RESOURCE_LINKAGE_ERROR" {print $2}' \
                  "$DIR/etl-errors-by-code.txt" | paste -sd, -)
  if [[ -z "$unknown_codes" ]]; then
    crit I7 FAILED pass "Allowed .error codes" "{RCE, RLE}" "ok"
  else
    crit I7 FAILED fail "Allowed .error codes" "{RCE, RLE}" "unknown: $unknown_codes"
  fi
else
  crit I7 FAILED skip "Allowed .error codes" "{RCE, RLE}" "missing etl-errors-by-code.txt"
fi

# ---------- I8. Severe (non-skip) OMOP errors == 0 ----------
if [[ -f "$DIR/omop-severe.txt" ]]; then
  if [[ "${SEVERE_LINES:-0}" -eq 0 ]]; then
    crit I8 FAILED pass "Severe OMOP errors" "== 0" "0"
  else
    crit I8 FAILED fail "Severe OMOP errors" "== 0" "$(fmt "$SEVERE_LINES")"
  fi
else
  crit I8 FAILED skip "Severe OMOP errors" "== 0" "missing omop-severe.txt"
fi

# ---------- I9. Aerospike stop-writes counters == 0 ----------
# Aerospike's asinfo emits `stop-writes-count=N` and `stop-writes-size=N`
# per set. Either being non-zero means the node refused writes for that
# set — a silent ingestion killer that never surfaces as a per-row error.
if [[ -f "$DIR/aerospike-sets.txt" ]]; then
  sw_breach=$(awk -F= '
    /^\[/ { set=$0; gsub(/[][]/,"",set); next }
    /^stop-writes-count=|^stop-writes-size=/ {
      v=$2; sub(/;.*$/,"",v)
      if (v+0 > 0) printf "%s.%s=%s ", set, $1, v
    }
  ' "$DIR/aerospike-sets.txt")
  if [[ -z "$sw_breach" ]]; then
    crit I9 FAILED pass "Aerospike stop-writes" "all 0" "0"
  else
    crit I9 FAILED fail "Aerospike stop-writes" "all 0" "${sw_breach% }"
  fi
else
  crit I9 FAILED skip "Aerospike stop-writes" "all 0" "missing aerospike-sets.txt"
fi

# ---------- I10. CSV rows ↔ Aerospike ExcelRecord parity ----------
# Only evaluated when the collector was run against a real upload CSV and
# therefore persisted `csv-rows.txt`. Confirms nothing was dropped between
# disk and the staging cache (multipart truncation, header parsing, …).
if [[ -s "$DIR/csv-rows.txt" && "${AS_EXCEL:-?}" =~ ^[0-9]+$ ]]; then
  CSV_ROWS=$(awk 'NR==1{print; exit}' "$DIR/csv-rows.txt" | tr -d ' ')
  if [[ "$CSV_ROWS" =~ ^[0-9]+$ ]]; then
    if [[ "$CSV_ROWS" == "$AS_EXCEL" ]]; then
      crit I10 FAILED pass "CSV rows ↔ Aerospike records" "equal" "$(fmt "$CSV_ROWS") / $(fmt "$AS_EXCEL")"
    else
      crit I10 FAILED fail "CSV rows ↔ Aerospike records" "equal" "$(fmt "$CSV_ROWS") / $(fmt "$AS_EXCEL")"
    fi
  else
    crit I10 FAILED skip "CSV rows ↔ Aerospike records" "equal" "csv-rows.txt unparseable"
  fi
else
  crit I10 FAILED skip "CSV rows ↔ Aerospike records" "equal" "no csv-rows.txt (CSV_FILE not set)"
fi

# ---------- S1. Pod health (restarts, status, readiness) ----------
# `kubectl get pods -o wide` emits: NAME READY STATUS RESTARTS AGE IP NODE ...
# so $2=READY, $3=STATUS, $4=RESTARTS. Any of:
#   - RESTARTS > 0
#   - STATUS not in {Running, Completed, Succeeded}
#   - READY not of the form N/N (container not ready)
# is a deployment health anomaly worth flagging.
if [[ -s "$DIR/pods.txt" ]]; then
  pod_issues=$(awk '
    NR == 1 { next }
    {
      name=$1; ready=$2; status=$3; restarts=$4+0
      bad=""
      if (restarts > 0)                                        bad = bad "restarts=" restarts " "
      if (status != "Running" && status != "Completed" && status != "Succeeded") bad = bad "status=" status " "
      # READY should be "N/N" with both sides equal.
      if (ready !~ /^[0-9]+\/[0-9]+$/) bad = bad "ready=" ready " "
      else {
        split(ready, r, "/")
        if (r[1] != r[2]) bad = bad "ready=" ready " "
      }
      if (bad != "") printf "%s(%s) ", name, bad
    }
  ' "$DIR/pods.txt" | sed 's/ )/)/g; s/  / /g')
  if [[ -z "$pod_issues" ]]; then
    crit S1 DEGRADED pass "Pod health" "all Running/1 & 0 restarts" "ok"
  else
    crit S1 DEGRADED fail "Pod health" "all Running/1 & 0 restarts" "${pod_issues% }"
  fi
else
  crit S1 DEGRADED skip "Pod health" "all Running/1 & 0 restarts" "missing pods.txt"
fi

# ---------- S2. No SILENT_LOSS ----------
if [[ -n "${recon:-}" ]]; then
  silent_count=$(echo "$recon" | awk '$6=="SILENT_LOSS" {c++} END{print c+0}')
  if [[ "$silent_count" -eq 0 ]]; then
    crit S2 DEGRADED pass "No SILENT_LOSS in staging" "0 resources" "0"
  else
    crit S2 DEGRADED fail "No SILENT_LOSS in staging" "0 resources" "${silent_count} (${silent% })"
  fi
else
  crit S2 DEGRADED skip "No SILENT_LOSS in staging" "0 resources" "no reconciliation table"
fi

# ---------- S3. No DUPLICATES ----------
if [[ -n "${recon:-}" ]]; then
  dup_count=$(echo "$recon" | awk '$6=="DUPLICATES" {c++} END{print c+0}')
  if [[ "$dup_count" -eq 0 ]]; then
    crit S3 DEGRADED pass "No DUPLICATES in staging" "0 resources" "0"
  else
    crit S3 DEGRADED fail "No DUPLICATES in staging" "0 resources" "${dup_count} (${dup% })"
  fi
else
  crit S3 DEGRADED skip "No DUPLICATES in staging" "0 resources" "no reconciliation table"
fi

# ---------- S4. No 100%-filter on core OMOP domains ----------
# A Spring Batch step that converts source data into OMOP is named
# `<source>To<Target>Step` (e.g. `drugsToDrugExposureStep`). Parse the
# target out of the name, snake_case it, and check whether it matches a
# core domain — either directly (`drug_exposure`) or with the OMOP
# `_occurrence` suffix (`Condition` → `condition_occurrence`). The earlier
# substring heuristic missed every `*_occurrence` and every non-alias
# target, so nearly all real 100%-filters slipped through.
if [[ -s "$DIR/omop-step-counts.txt" ]]; then
  core_hit=$(awk -v domains="$SUCCESS_CORE_OMOP_DOMAINS" '
    function to_snake(s,   r, c, i) {
      r = ""
      for (i = 1; i <= length(s); i++) {
        c = substr(s, i, 1)
        if (c ~ /[A-Z]/ && i > 1) r = r "_"
        r = r tolower(c)
      }
      return r
    }
    BEGIN {
      n = split(domains, d, /[ \t,]+/)
      for (i = 1; i <= n; i++) core[tolower(d[i])] = 1
    }
    NR > 1 && $2 > 0 && $3 == 0 {
      step = $1
      target = step
      sub(/.*To/, "", target)   # strip everything up to (and incl.) "To"
      sub(/Step$/, "", target)  # strip trailing "Step"
      snake = to_snake(target)
      alias = snake "_occurrence"
      if (snake in core)      { printf "%s→%s ", step, snake; next }
      else if (alias in core) { printf "%s→%s ", step, alias; next }
    }
  ' "$DIR/omop-step-counts.txt")
  if [[ -z "$core_hit" ]]; then
    crit S4 DEGRADED pass "No 100%-filter on core OMOP domain" "core domains survive" "ok"
  else
    crit S4 DEGRADED fail "No 100%-filter on core OMOP domain" "core domains survive" "${core_hit% }"
  fi
else
  crit S4 DEGRADED skip "No 100%-filter on core OMOP domain" "core domains survive" "missing omop-step-counts.txt"
fi

# ---------- S5. No non-retry OMOP skips ----------
if [[ -n "${REAL_SKIP:-}" ]]; then
  if [[ "$REAL_SKIP" -eq 0 ]]; then
    crit S5 DEGRADED pass "No non-retry OMOP skips" "== 0" "0"
  else
    crit S5 DEGRADED fail "No non-retry OMOP skips" "== 0" "$(fmt "$REAL_SKIP")"
  fi
else
  crit S5 DEGRADED skip "No non-retry OMOP skips" "== 0" "no omop-skip-by-type.txt"
fi

# ---------- S6. No linkage abort ----------
if [[ -n "${UNSAVED:-}" ]]; then
  crit S6 DEGRADED fail "No linkage abort (Unsaved resources)" "Unsaved == 0" "$(fmt "$UNSAVED")"
else
  crit S6 DEGRADED pass "No linkage abort (Unsaved resources)" "Unsaved == 0" "0"
fi

# ---------- S7. No zero OMOP domain count (among core list) ----------
# Catches the "staging had rows but OMOP domain ended up empty" scenario
# that S4 alone misses when no single Spring Batch step is 100%-filtered
# but the combination of filters leaves the destination domain empty.
if [[ -n "$OMOP_DOMAINS_KV" ]]; then
  zero_core=""
  for d in $SUCCESS_CORE_OMOP_DOMAINS; do
    v=$(awk -F= -v k="$d" '$1==k{print $2; exit}' <<<"$OMOP_DOMAINS_KV")
    [[ "$v" == "0" ]] && zero_core+="${d} "
  done
  if [[ -z "$zero_core" ]]; then
    crit S7 DEGRADED pass "No zero OMOP domain (core list)" "count > 0" "ok"
  else
    crit S7 DEGRADED fail "No zero OMOP domain (core list)" "count > 0" "${zero_core% }"
  fi
else
  crit S7 DEGRADED skip "No zero OMOP domain (core list)" "count > 0" "missing omop-counts.txt"
fi

# ---------- B1. Wrong-number-format rows within budget ----------
if [[ -n "${WNF:-}" ]]; then
  if [[ "$WNF" -le "$SUCCESS_BUDGET_WNF_ROWS" ]]; then
    crit B1 DEGRADED pass "Wrong-number-format rows" "≤ ${SUCCESS_BUDGET_WNF_ROWS}" "$(fmt "$WNF")"
  else
    crit B1 DEGRADED fail "Wrong-number-format rows" "≤ ${SUCCESS_BUDGET_WNF_ROWS}" "$(fmt "$WNF")"
  fi
else
  crit B1 DEGRADED skip "Wrong-number-format rows" "≤ ${SUCCESS_BUDGET_WNF_ROWS}" "no etl-idea.log"
fi

# ---------- B2. Linkage-abort rate within budget ----------
if [[ -n "${UNSAVED:-}" && "${API_RECORDS:-?}" =~ ^[0-9]+$ && "${API_RECORDS}" -gt 0 ]]; then
  rate=$(awk -v u="$UNSAVED" -v t="$API_RECORDS" 'BEGIN { printf "%.6f", u/t }')
  measured="$(pct3 "$rate") ($(fmt "$UNSAVED")/$(fmt "$API_RECORDS"))"
  if le_num "$rate" "$SUCCESS_BUDGET_LINKAGE_RATE"; then
    crit B2 DEGRADED pass "Linkage-abort rate" "≤ $(pct3 "$SUCCESS_BUDGET_LINKAGE_RATE")" "$measured"
  else
    crit B2 DEGRADED fail "Linkage-abort rate" "≤ $(pct3 "$SUCCESS_BUDGET_LINKAGE_RATE")" "$measured"
  fi
else
  crit B2 DEGRADED skip "Linkage-abort rate" "≤ $(pct3 "$SUCCESS_BUDGET_LINKAGE_RATE")" "n/a"
fi

# ---------- B3. Per-resource errored-rate budgets (CORE vs OTHER) ----------
if [[ -n "${recon:-}" ]]; then
  core_breaches=""
  other_breaches=""
  while IFS= read -r row; do
    res=$(awk '{print $1}' <<<"$row")
    r=$(awk  '{print $2+0}' <<<"$row")
    e=$(awk  '{print $3+0}' <<<"$row")
    [[ "$res" == "resource" || "$r" -le 0 ]] && continue
    rate=$(awk -v e="$e" -v r="$r" 'BEGIN { printf "%.6f", e/r }')
    if [[ " $SUCCESS_CORE_RESOURCES " == *" $res "* ]]; then
      if ! le_num "$rate" "$SUCCESS_BUDGET_ERRORED_RATE_CORE"; then
        core_breaches+="${res}($(pct3 "$rate")) "
      fi
    else
      if ! le_num "$rate" "$SUCCESS_BUDGET_ERRORED_RATE_OTHER"; then
        other_breaches+="${res}($(pct3 "$rate")) "
      fi
    fi
  done <<<"$recon"
  if [[ -z "$core_breaches" ]]; then
    crit B3 DEGRADED pass "CORE resource errored-rate" "≤ $(pct3 "$SUCCESS_BUDGET_ERRORED_RATE_CORE")" "ok (${SUCCESS_CORE_RESOURCES})"
  else
    crit B3 DEGRADED fail "CORE resource errored-rate" "≤ $(pct3 "$SUCCESS_BUDGET_ERRORED_RATE_CORE")" "${core_breaches% }"
  fi
  if [[ -z "$other_breaches" ]]; then
    crit B4 DEGRADED pass "Other resource errored-rate" "≤ $(pct3 "$SUCCESS_BUDGET_ERRORED_RATE_OTHER")" "ok"
  else
    # Truncate to avoid swamping the table when every resource breaches.
    sample=$(echo "$other_breaches" | tr ' ' '\n' | head -5 | tr '\n' ' ')
    count=$(echo "$other_breaches" | wc -w | tr -d ' ')
    crit B4 DEGRADED fail "Other resource errored-rate" "≤ $(pct3 "$SUCCESS_BUDGET_ERRORED_RATE_OTHER")" "${count} resources breach (${sample%% })"
  fi
else
  crit B3 DEGRADED skip "CORE resource errored-rate"  "≤ $(pct3 "$SUCCESS_BUDGET_ERRORED_RATE_CORE")"  "no reconciliation table"
  crit B4 DEGRADED skip "Other resource errored-rate" "≤ $(pct3 "$SUCCESS_BUDGET_ERRORED_RATE_OTHER")" "no reconciliation table"
fi

# ---------- B5. OMOP real-skip rate ----------
if [[ -n "${REAL_SKIP:-}" && -n "${TOT_READ:-}" && "$TOT_READ" -gt 0 ]]; then
  rate=$(awk -v s="$REAL_SKIP" -v r="$TOT_READ" 'BEGIN { printf "%.6f", s/r }')
  measured="$(pct3 "$rate") ($(fmt "$REAL_SKIP")/$(fmt "$TOT_READ"))"
  if le_num "$rate" "$SUCCESS_BUDGET_OMOP_SKIP_RATE"; then
    crit B5 DEGRADED pass "OMOP real-skip rate" "≤ $(pct3 "$SUCCESS_BUDGET_OMOP_SKIP_RATE")" "$measured"
  else
    crit B5 DEGRADED fail "OMOP real-skip rate" "≤ $(pct3 "$SUCCESS_BUDGET_OMOP_SKIP_RATE")" "$measured"
  fi
else
  crit B5 DEGRADED skip "OMOP real-skip rate" "≤ $(pct3 "$SUCCESS_BUDGET_OMOP_SKIP_RATE")" "n/a"
fi

# ---------- Emit the criteria table ----------
emit "_Code prefix: **I**\* = hard invariant (any ✗ ⇒ FAILED) · **S**\* = soft invariant (any ✗ ⇒ DEGRADED) · **B**\* = budget (any ✗ ⇒ DEGRADED)._"
emit
emit "| Code | Criterion | Rule | Measured | Result |"
emit "|------|-----------|------|----------|:------:|"
# Emit each captured row. `CRIT_ROWS_MD` already carries the trailing newlines.
while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  emit "$line"
done <<<"${CRIT_ROWS_MD%$'\n'}"

# ---------- Follow-ups ----------
# Exit code still encodes the worst tier (0=all pass, 1=soft/budget fail,
# 2=hard-invariant fail) so this script stays useful in CI; no verdict
# line is rendered in the markdown itself.
case $WORST_TIER in
  0) EXIT_CODE=0 ;;
  1) EXIT_CODE=1 ;;
  2) EXIT_CODE=2 ;;
esac

emit
if [[ -n "$FAIL_REASONS" ]]; then
  emit "**Follow-ups:**"
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    emit "$line"
  done <<<"${FAIL_REASONS%$'\n'}"
else
  emit "$(green "No follow-ups — all criteria pass.") ✓"
fi

emit
emit "_Budgets & core lists are overridable in \`audit.conf\` via \`SUCCESS_BUDGET_*\` and \`SUCCESS_CORE_*\`._"

# =======================================================================
# Write markdown copy if requested
# =======================================================================
if [[ -n "$MD_OUT" ]]; then
  mkdir -p "$(dirname "$MD_OUT")"
  printf "%s" "$_plain" > "$MD_OUT"
  echo
  echo "Markdown summary written to: $MD_OUT"
fi

exit "${EXIT_CODE:-0}"
