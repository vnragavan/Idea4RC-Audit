# audit-config-lib.sh
# shellcheck shell=bash
#
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 nara
#
# Shared config loader for the Capsule audit scripts.
#
# Usage from a script:
#   SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   . "$SCRIPT_DIR/audit-config-lib.sh"
#   load_audit_config "$@"           # consumes -c/--config and sets $_AUDIT_REMAINING_ARGS
#   set -- "${_AUDIT_REMAINING_ARGS[@]}"
#
# Precedence (highest to lowest):
#   1. Environment variables set before invocation
#      (e.g. `OUTDIR=/tmp/x ./audit-pipeline-errors.sh -c foo.conf`)
#   2. Values in the config file
#   3. Built-in defaults in this lib
#
# After loading, the following variables are guaranteed to be set:
#
#   NS KUBECTL OUTDIR CSV_FILE
#   ETL_APP_SELECTOR ETL_DB_SELECTOR OMOP_ETL_SELECTOR
#   OMOP_DB_POD_PATTERN AERO_POD_PATTERN
#   ETL_SERVICE ETL_PORT AUDIT_ERRORS_PATH AUDIT_RECORDS_PATH
#   AERO_NAMESPACE AERO_SET_RECORDS AERO_SET_ERRORS
#   ETL_DB_USER ETL_DB_NAME ETL_DB_PATIENT_TABLE
#   RESOURCE_TO_TABLE_OVERRIDE
#   OMOP_DB_USER OMOP_DB_NAME OMOP_SCHEMA
#   SUCCESS_CORE_RESOURCES SUCCESS_CORE_OMOP_DOMAINS
#   SUCCESS_BUDGET_WNF_ROWS
#   SUCCESS_BUDGET_ERRORED_RATE_CORE SUCCESS_BUDGET_ERRORED_RATE_OTHER
#   SUCCESS_BUDGET_LINKAGE_RATE SUCCESS_BUDGET_OMOP_SKIP_RATE

# Keys whose pre-invocation environment value (if any) must win over
# the value in the config file. Kept in one place so it's easy to
# extend — add any new config key you want env-overridable here.
_AUDIT_ENV_OVERRIDABLE_KEYS=(
  NS KUBECTL OUTDIR CSV_FILE
  ETL_APP_SELECTOR ETL_DB_SELECTOR OMOP_ETL_SELECTOR
  OMOP_DB_POD_PATTERN AERO_POD_PATTERN
  ETL_SERVICE ETL_PORT AUDIT_ERRORS_PATH AUDIT_RECORDS_PATH
  AERO_NAMESPACE AERO_SET_RECORDS AERO_SET_ERRORS
  ETL_DB_USER ETL_DB_NAME ETL_DB_PATIENT_TABLE
  RESOURCE_TO_TABLE_OVERRIDE
  OMOP_DB_USER OMOP_DB_NAME OMOP_SCHEMA
  SUCCESS_CORE_RESOURCES SUCCESS_CORE_OMOP_DOMAINS
  SUCCESS_BUDGET_WNF_ROWS
  SUCCESS_BUDGET_ERRORED_RATE_CORE SUCCESS_BUDGET_ERRORED_RATE_OTHER
  SUCCESS_BUDGET_LINKAGE_RATE SUCCESS_BUDGET_OMOP_SKIP_RATE
)

_audit_find_default_config() {
  # 1. CAPSULE_AUDIT_CONFIG env var
  if [[ -n "${CAPSULE_AUDIT_CONFIG:-}" && -f "${CAPSULE_AUDIT_CONFIG}" ]]; then
    echo "$CAPSULE_AUDIT_CONFIG"
    return 0
  fi
  # 2. audit.conf next to the calling script
  local caller_dir
  caller_dir="$(cd "$(dirname "${BASH_SOURCE[1]}")" && pwd)"
  if [[ -f "$caller_dir/audit.conf" ]]; then
    echo "$caller_dir/audit.conf"
    return 0
  fi
  # 3. audit.conf in CWD
  if [[ -f "./audit.conf" ]]; then
    echo "./audit.conf"
    return 0
  fi
  return 1
}

load_audit_config() {
  local config=""
  _AUDIT_REMAINING_ARGS=()

  # Extract -c/--config (and its value) without disturbing the other args.
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -c|--config)
        config="$2"; shift 2 ;;
      --config=*)
        config="${1#--config=}"; shift ;;
      -c*)
        config="${1#-c}"; shift ;;
      *)
        _AUDIT_REMAINING_ARGS+=("$1"); shift ;;
    esac
  done

  if [[ -z "$config" ]]; then
    config="$(_audit_find_default_config || true)"
  fi

  if [[ -z "$config" ]]; then
    cat >&2 <<EOF
No audit config file provided.

Pass one explicitly:
  $(basename "$0") -c /path/to/audit.conf [...]

Or set CAPSULE_AUDIT_CONFIG, or place audit.conf next to the script.
A template lives at: $(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/audit.conf.example
EOF
    return 1
  fi

  if [[ ! -f "$config" ]]; then
    echo "Config file not found: $config" >&2
    return 1
  fi

  # Snapshot env values that were set BEFORE we source the config so
  # we can restore them afterwards — env-set values must win over the
  # config file (documented precedence).
  local _k _snap_name
  for _k in "${_AUDIT_ENV_OVERRIDABLE_KEYS[@]}"; do
    if [[ -n "${!_k+x}" ]]; then
      _snap_name="_AUDIT_ENV_SNAPSHOT_${_k}"
      printf -v "$_snap_name" '%s' "${!_k}"
    fi
  done

  # shellcheck disable=SC1090
  . "$config"
  AUDIT_CONFIG_PATH="$config"

  # Re-apply the env snapshot so pre-invocation env wins over config.
  for _k in "${_AUDIT_ENV_OVERRIDABLE_KEYS[@]}"; do
    _snap_name="_AUDIT_ENV_SNAPSHOT_${_k}"
    if [[ -n "${!_snap_name+x}" ]]; then
      printf -v "$_k" '%s' "${!_snap_name}"
      unset "$_snap_name"
    fi
  done

  # Defaults for anything not set by the config (or by the caller's env).
  #
  # Note: the defaults below are EXAMPLE values taken from the
  # reference IDEA4RC install (namespace, user names, DB names,
  # Aerospike namespace). They are here as a last-resort fallback so
  # the scripts still do something reasonable when run against that
  # reference install; every real deployment should override them in
  # `audit.conf`.
  : "${NS:=datamesh}"
  : "${KUBECTL:=kubectl}"
  : "${OUTDIR:=${CAPSULE_AUDIT_OUT:-./capsule-audit-latest}}"
  : "${CSV_FILE:=}"

  : "${ETL_APP_SELECTOR:=app=etl-idea}"
  : "${ETL_DB_SELECTOR:=app=etl}"
  : "${OMOP_ETL_SELECTOR:=app=omop-etl}"
  : "${OMOP_DB_POD_PATTERN:=omop-cdm}"
  : "${AERO_POD_PATTERN:=aerospike}"

  : "${ETL_SERVICE:=svc/etl-svc}"
  : "${ETL_PORT:=4001}"
  : "${AUDIT_ERRORS_PATH:=/audit/etl-errors}"
  : "${AUDIT_RECORDS_PATH:=/audit/records}"

  : "${AERO_NAMESPACE:=idea4rc}"
  : "${AERO_SET_RECORDS:=ExcelRecord}"
  : "${AERO_SET_ERRORS:=EtlProcessError}"

  : "${ETL_DB_USER:=etl}"
  : "${ETL_DB_NAME:=etl}"
  : "${ETL_DB_PATIENT_TABLE:=public.patient}"
  : "${RESOURCE_TO_TABLE_OVERRIDE:=}"

  : "${OMOP_DB_USER:=cdm_idea}"
  : "${OMOP_DB_NAME:=omopdb}"
  : "${OMOP_SCHEMA:=cdm_idea}"

  # Success-criteria budgets consumed by audit-summarize.sh. Conservative
  # defaults suitable for a mature deployment; tune per site in audit.conf.
  : "${SUCCESS_CORE_RESOURCES:=Patient Diagnosis}"
  : "${SUCCESS_CORE_OMOP_DOMAINS:=person visit_occurrence condition_occurrence procedure_occurrence measurement observation}"
  : "${SUCCESS_BUDGET_WNF_ROWS:=50}"
  : "${SUCCESS_BUDGET_ERRORED_RATE_CORE:=0.01}"
  : "${SUCCESS_BUDGET_ERRORED_RATE_OTHER:=0.05}"
  : "${SUCCESS_BUDGET_LINKAGE_RATE:=0.01}"
  : "${SUCCESS_BUDGET_OMOP_SKIP_RATE:=0.0001}"

  export NS KUBECTL OUTDIR CSV_FILE \
         ETL_APP_SELECTOR ETL_DB_SELECTOR OMOP_ETL_SELECTOR \
         OMOP_DB_POD_PATTERN AERO_POD_PATTERN \
         ETL_SERVICE ETL_PORT AUDIT_ERRORS_PATH AUDIT_RECORDS_PATH \
         AERO_NAMESPACE AERO_SET_RECORDS AERO_SET_ERRORS \
         ETL_DB_USER ETL_DB_NAME ETL_DB_PATIENT_TABLE \
         RESOURCE_TO_TABLE_OVERRIDE \
         OMOP_DB_USER OMOP_DB_NAME OMOP_SCHEMA \
         SUCCESS_CORE_RESOURCES SUCCESS_CORE_OMOP_DOMAINS \
         SUCCESS_BUDGET_WNF_ROWS \
         SUCCESS_BUDGET_ERRORED_RATE_CORE SUCCESS_BUDGET_ERRORED_RATE_OTHER \
         SUCCESS_BUDGET_LINKAGE_RATE SUCCESS_BUDGET_OMOP_SKIP_RATE \
         AUDIT_CONFIG_PATH

  return 0
}
