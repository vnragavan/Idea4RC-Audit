# Capsule Audit Scripts

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Shell: bash 3.2+](https://img.shields.io/badge/shell-bash%203.2%2B-blue.svg)](#dependencies)

A pair of bash scripts that audit a Capsule data-ingestion pipeline
(`etl-idea` → staging Postgres / Aerospike → `omop-etl` → OMOP CDM
Postgres) and produce both a raw artifact bundle and a human-readable
report.

The scripts are **generic across Capsule installations** — all
machine-/deployment-specific values (namespace, pod labels, DB users,
ports, audit paths, Aerospike set names, etc.) live in a separate
config file that is passed in at run time.

> **Warning — sensitive data.** The artifact bundle these scripts
> produce (`$OUTDIR/*.log`, `etl-errors.json`, `etl-records.json`,
> `*-samples.tsv`, …) contains raw rows / error messages from your
> pipeline and therefore almost certainly contains PHI / PII. **Never
> commit `$OUTDIR` to version control.** A `.gitignore` that excludes
> the default `capsule-audit-latest/` directory is shipped with this
> repo.

---

## Contents

- [Overview](#overview)
- [File layout](#file-layout)
- [Dependencies](#dependencies)
- [Configuration](#configuration)
- [Usage](#usage)
- [When to run](#when-to-run)
- [Artifacts produced](#artifacts-produced)
- [Reading the summary report](#reading-the-summary-report)
- [Troubleshooting](#troubleshooting)
- [Extending / porting to a new deployment](#extending--porting-to-a-new-deployment)
- [Design notes](#design-notes)
- [Contributing](#contributing)
- [Security](#security)
- [License](#license)

---

## Overview

There are two scripts with a strict separation of concerns:

| Script | Network access | Purpose |
|---|---|---|
| `audit-pipeline-errors.sh` | **Online** — talks to the cluster via `kubectl` | Collects logs, DB counts, Aerospike set sizes, and audit-API dumps into `$OUTDIR` |
| `audit-summarize.sh`       | **Offline** — reads files only | Renders a human-readable report (optionally Markdown) from the artifacts in `$OUTDIR` |

Reasons for the split:

- The collector needs cluster credentials, a working `kubectl`, and can
  take minutes on a large deployment. Run it infrequently.
- The summarizer is pure file parsing. Re-run it as many times as you
  like (e.g. after tweaking thresholds in the script) without touching
  the cluster.
- Artifacts under `$OUTDIR` are portable — you can tarball them, ship
  them to a colleague, and they can run the summarizer locally.

---

## File layout

```
audit/
├── README.md                  # this file
├── audit.conf.example         # template config; copy & edit per cluster
├── audit-config-lib.sh        # shared loader; sourced by both scripts
├── audit-pipeline-errors.sh   # collector (online, touches the cluster)
└── audit-summarize.sh         # reporter (offline, reads artifacts)
```

All three shell files need to live in the same directory — the scripts
auto-detect `audit-config-lib.sh` next to themselves.

---

## Dependencies

### On the operator's machine (where the scripts run)

Required:

- **bash ≥ 3.2** — the scripts deliberately avoid bash-4-only features
  (`declare -A`, `mapfile`, case-modification expansions, `coproc`,
  etc.), so the stock `/bin/bash` on macOS works as-is. No special
  install needed on Linux.
- **kubectl** (or a flavored variant like `microk8s.kubectl`; see
  `KUBECTL` in the config) with valid credentials for the target
  cluster and the target namespace.
- **awk**, **sed**, **grep**, **sort**, **uniq**, **wc**, **head**,
  **tail**, **find**, **tr**, **cut** — any POSIX-ish box has these.
- **curl** — used to hit the audit API through the port-forward.
- **jq** — strongly recommended. Without it, the `etl-errors.json`
  breakdown sections are skipped and the script falls back to "install
  jq for a detailed breakdown".

Optional:

- A CSV viewer (if you want to eyeball `wrong-number-format-rows.tsv`,
  `etl-errors-samples.tsv`, etc.).

The scripts check for `kubectl`, `awk`, `sed`, `grep`, `sort`, `uniq`
up front and exit with a clear error if anything is missing. `jq` and
`curl` are checked softly — sections that need them are skipped with a
warning if they're unavailable.

### In the target cluster

The collector assumes the pipeline looks roughly like this:

| Component         | Default selector / pattern | What the collector does          |
|-------------------|----------------------------|----------------------------------|
| `etl-idea` app    | `app=etl-idea`             | `logs`, `describe`, port-forward |
| Staging Postgres  | `app=etl`                  | `exec psql` against `etl` DB     |
| `omop-etl` batch  | `app=omop-etl`             | `logs`                           |
| OMOP Postgres     | pod name matches `omop-cdm`| `exec psql` against `omopdb`     |
| Aerospike         | pod name matches `aerospike` | `exec asinfo` against `idea4rc` |

All of these selectors/patterns/DB names/schema names are configurable
(see next section). The pods themselves need:

- `psql` on the Postgres pods' `PATH`.
- `asinfo` on the Aerospike pod's `PATH`.
- The `etl-idea` service exposing `/audit/etl-errors` and
  `/audit/records` **without auth** when reached from inside the
  cluster (the collector uses `kubectl port-forward`, so it talks
  directly to the pod/service and bypasses any ingress-level auth).

No changes are ever made to the cluster. The collector only reads.

---

## Configuration

Every value that could differ between installations lives in an
external bash config file. The scripts ship with
`audit.conf.example` as a fully-commented template.

### Setting up a config for a new cluster

```bash
cd /path/to/audit
cp audit.conf.example my-cluster.conf
$EDITOR my-cluster.conf
```

### How the scripts locate the config

Resolution order (first match wins):

1. `-c /path/to/my-cluster.conf` command-line flag
2. `--config=/path/to/my-cluster.conf`
3. `CAPSULE_AUDIT_CONFIG=/path/to/my-cluster.conf` environment variable
4. `audit.conf` in the **same directory as the script**
5. `audit.conf` in the **current working directory**

If none are found, the script exits with a helpful error that points
at `audit.conf.example`.

### Every key

The config file is sourced as bash (so comments start with `#`, strings
don't need quotes unless they contain spaces). Every key has a
documented default — you only need to override the ones that differ
from the IDEA4RC reference deployment.

| Key | Default | Meaning |
|---|---|---|
| `NS` | `datamesh` | Kubernetes namespace containing the pipeline |
| `KUBECTL` | `kubectl` (the example config uses `microk8s.kubectl`) | Path / name of the kubectl binary |
| `OUTDIR` | `./capsule-audit-latest` | Where artifacts are written and read |
| `CSV_FILE` | _(empty)_ | Optional: path to the uploaded CSV for row-count reconciliation |
| `ETL_APP_SELECTOR` | `app=etl-idea` | Label selector for the etl-idea app pod(s) |
| `ETL_DB_SELECTOR` | `app=etl` | Label selector for the staging Postgres pod |
| `OMOP_ETL_SELECTOR` | `app=omop-etl` | Label selector for the OMOP ETL batch pod |
| `OMOP_DB_POD_PATTERN` | `omop-cdm` | Substring that must appear in the OMOP Postgres pod name |
| `AERO_POD_PATTERN` | `aerospike` | Substring that must appear in the Aerospike pod name |
| `ETL_SERVICE` | `svc/etl-svc` | `kubectl port-forward` target for the audit API |
| `ETL_PORT` | `4001` | Port used for the port-forward (local **and** remote) |
| `AUDIT_ERRORS_PATH` | `/audit/etl-errors` | Audit API path that returns the error list |
| `AUDIT_RECORDS_PATH` | `/audit/records` | Audit API path that returns the processed-record list |
| `AERO_NAMESPACE` | `idea4rc` | Aerospike namespace (`asinfo -v "sets/<ns>/..."`) |
| `AERO_SET_RECORDS` | `ExcelRecord` | Aerospike set name for processed records |
| `AERO_SET_ERRORS` | `EtlProcessError` | Aerospike set name for errors |
| `ETL_DB_USER` | `etl` | Username for the staging Postgres `psql` connection |
| `ETL_DB_NAME` | `etl` | Database name on the staging Postgres |
| `ETL_DB_PATIENT_TABLE` | `public.patient` | Fully-qualified staging table used for the patient handshake count |
| `OMOP_DB_USER` | `cdm_idea` | Username for the OMOP Postgres `psql` connection |
| `OMOP_DB_NAME` | `omopdb` | Database name on the OMOP Postgres |
| `OMOP_SCHEMA` | `cdm_idea` | Schema that holds the OMOP CDM tables |
| `RESOURCE_TO_TABLE_OVERRIDE` | _(empty)_ | Manual overrides for the per-resource reconciliation's resource→table mapping. Space-separated `Name:table_name` pairs. Only needed for tables whose name isn't the resource's CamelCase name converted to snake_case. |
| `SUCCESS_CORE_RESOURCES` | `Patient Diagnosis` | Space-separated list of resources whose errored-rate is judged against the stricter `CORE` budget. |
| `SUCCESS_CORE_OMOP_DOMAINS` | `person visit_occurrence condition_occurrence procedure_occurrence measurement observation` | OMOP CDM tables whose 100%-filter or zero-count drop downgrades the run. |
| `SUCCESS_BUDGET_WNF_ROWS` | `50` | Max `Wrong number format` rows (log-derived) before criterion `B1` fails. |
| `SUCCESS_BUDGET_ERRORED_RATE_CORE` | `0.01` | Max `errored / records` ratio for CORE resources (criterion `B3`). |
| `SUCCESS_BUDGET_ERRORED_RATE_OTHER` | `0.05` | Max `errored / records` ratio for non-CORE resources (criterion `B4`). |
| `SUCCESS_BUDGET_LINKAGE_RATE` | `0.01` | Max `Unsaved resources / api_records` ratio (criterion `B2`). |
| `SUCCESS_BUDGET_OMOP_SKIP_RATE` | `0.0001` | Max `real_skip / total_read` ratio across OMOP steps (criterion `B5`). |

### Environment overrides

A handful of values can be overridden with environment variables for
one-off runs without editing the config file:

```bash
OUTDIR=/tmp/debug-run CSV_FILE=/tmp/upload.csv \
  ./audit-pipeline-errors.sh -c my-cluster.conf
```

Environment variables take precedence over values in the config file.
The loader snapshots any pre-set env values for known keys, sources
the config, then restores the snapshot — so `OUTDIR=/tmp/debug-run`
wins over whatever `OUTDIR=` is inside `my-cluster.conf`, and any key
you don't pre-set falls through to the config value (and then to the
lib default).

---

## Usage

### Collecting audit data (online)

```bash
# Minimal — config passed explicitly:
./audit-pipeline-errors.sh -c /path/to/my-cluster.conf

# With CSV reconciliation:
./audit-pipeline-errors.sh -c my-cluster.conf CSV_FILE=/tmp/upload.csv

# One-off output directory:
OUTDIR=/tmp/today ./audit-pipeline-errors.sh -c my-cluster.conf
```

The collector prints a live report while it runs and writes all raw
data into `$OUTDIR`. It takes roughly 10–60 seconds against a healthy
cluster; longer if `etl-idea.log` or `omop-etl.log` are very large.

### Generating a summary (offline)

```bash
# Default — reads $OUTDIR from the same config, renders to stdout:
./audit-summarize.sh -c my-cluster.conf

# Also write SUMMARY.md inside $OUTDIR:
./audit-summarize.sh -c my-cluster.conf -m

# Summarize a specific previous run directory:
./audit-summarize.sh -c my-cluster.conf /archive/audit-2026-04-21

# Write the markdown to an arbitrary path:
./audit-summarize.sh -c my-cluster.conf -o /tmp/report.md
```

Flags supported by `audit-summarize.sh`:

| Flag | Meaning |
|---|---|
| `-c`, `--config <file>` | Config file (same semantics as the collector) |
| `-m`, `--markdown` | Write a plain-markdown copy to `$DIR/SUMMARY.md` |
| `-o`, `--output <file>` | Write the markdown copy to a specific path |
| `-h`, `--help` | Print the header comment block |
| _(positional)_ | Override `$OUTDIR` for this run only |

The summarizer returns an exit code that encodes the worst failing
success-criterion tier, so it's directly usable in CI:

| Exit code | Meaning |
|:---:|---|
| `0` | Every success criterion passes. |
| `1` | One or more soft-invariant (`S*`) or budget (`B*`) criteria fail — follow-up required but the run is still usable. |
| `2` | One or more hard-invariant (`I*`) criteria fail — the run is not trustworthy. |

The report itself does not render a "Verdict" line; the criteria table
and Follow-ups list are the outcome.

---

## When to run

Typical triggers for the collector:

- **After every CSV upload you want to audit.** Run it once the
  upload has finished, the Aerospike staging set has stabilized, and
  the `omop-etl` job has exited (check `kubectl get pods` — the job
  pod should be `Completed`). Running earlier will give you partial
  `STEP_COUNT` tables and misleading skip counts.
- **After a pipeline deploy / version bump**, to confirm the new
  image hasn't regressed error handling.
- **During incident response**, to snapshot the current state of the
  pipeline into a timestamped directory before anything changes.
- **Before clearing staging / truncating tables** — the artifact
  bundle is the only forensic record you'll have afterwards.

The summarizer can be re-run at any time:

- After you tweak a threshold in `audit-summarize.sh`.
- To produce a Markdown copy to paste into a ticket.
- To re-summarize an archived run that a colleague sent you.

---

## Artifacts produced

All artifacts land under `$OUTDIR` (default `./capsule-audit-latest`,
relative to your current working directory; override in the config).
The collector wipes the directory's top-level files at the start of
each run to avoid mixing artifacts from different uploads.

> **Reminder:** `$OUTDIR` contains raw logs, record-level JSON and
> sample rows pulled straight from the pipeline — treat it as
> **sensitive data**, don't commit it, and delete old runs when you
> no longer need them. The shipped `.gitignore` already excludes the
> default directory.

| File | Produced by | Content |
|---|---|---|
| `pods.txt` | `kubectl get pods -o wide` | Pod snapshot incl. restart counts |
| `etl-idea.log` | `kubectl logs -l app=etl-idea` | Full etl-idea app log |
| `aerospike-sets.txt` | `asinfo -v "sets/<ns>/<set>"` | Aerospike set stats (objects, bytes) |
| `etl-errors.json` | `curl /audit/etl-errors` | Raw audit error list |
| `etl-records.json` | `curl /audit/records` | Raw processed-record list |
| `etl-errors.probe` | `curl` to alternate paths | Only written if the main audit URL 404s |
| `etl-errors-by-code.txt` | `jq + sort \| uniq -c` | Error counts grouped by `error` code |
| `etl-errors-by-resource.txt` | same | Grouped by `resourceName` |
| `etl-errors-by-property.txt` | same | Grouped by `propertyInError` |
| `etl-errors-by-motivation.txt` | same | Grouped by motivation/message prefix |
| `etl-errors-samples.tsv` | `jq` | First 10 errors as a TSV sample |
| `etl-records-by-resource.txt` | `jq + sort -u + awk` | Resource **instance** counts — distinct `recordId` per resource prefix of `coreVariable` (records API is field-level, so instances must be derived) |
| `etl-errors-by-resource-instances.txt` | same | Distinct **errored** instances per resource (`recordId` per `resourceName` in errors.json). Used to distinguish "rows missing because they errored" from silent loss |
| `staging-counts.txt` | `psql` (staging DB) | `public.*` table row counts |
| `omop-etl.log` | `kubectl logs -l app=omop-etl` | Full OMOP ETL log |
| `omop-severe.txt` | grep of `omop-etl.log` | Severe (non-skip) errors |
| `omop-step-counts.txt` | grep + awk | Per-step read/write/filter/skip table |
| `omop-skip-by-type.txt` | grep + sort \| uniq -c | `EXCEPTION SKIP` counts by exception type + step |
| `omop-skip-ids.txt` | grep + sed | Distinct (step, source_id) pairs skipped |
| `omop-counts.txt` | `psql` (OMOP DB) | Domain counts (person, visit_occurrence, …) |
| `wrong-number-format-rows.tsv` | `audit-summarize.sh` | Extracted "Wrong number format" rows (`record_id, row_id, property_in_error, bad_value, parent_id`) |
| `blocked-record-ids.txt` | `audit-summarize.sh` | Record IDs blocked by the Stage 1 global `RESOURCE_LINKAGE_ERROR` (one ID per line, extracted from the log's `Blocked record_id list (all): [...]` line). Only produced when a linkage abort occurred. |
| `csv-rows.txt` | `audit-pipeline-errors.sh` | CSV row count (excluding the header). Only written when `CSV_FILE` is configured; drives criterion `I10` (CSV ↔ Aerospike parity). |
| `SUMMARY.md` | `audit-summarize.sh -m` | Markdown copy of the report |

Artifacts are plain text. Tar them up to preserve a run:

```bash
tar -czf audit-$(date +%Y%m%d-%H%M).tgz -C $(dirname "$OUTDIR") $(basename "$OUTDIR")
```

---

## Reading the summary report

The summary has seven sections. The bits worth internalizing:

### Stage 1 — `etl-idea`

- **"Real HTTP-level upload errors"** is strict on purpose — it only
  counts `Payload Too Large`, `MaxUploadSizeExceededException`,
  `MultipartException`, and friends. A bare `413` elsewhere in the log
  (e.g. a row ID or IP address) is ignored.
- **"Wrong-number-format rows"** pulls the offending
  `ExcelRecord(...)` line that follows each `Wrong number format:`
  error and tabulates them into `wrong-number-format-rows.tsv`. Quick
  way to find the handful of cells that need fixing in the source CSV.
- **"Global linkage abort (log-only)"** surfaces the `Unsaved
  resources: N` signal. This is the single most important log-only
  number, because the audit API represents the whole abort as one
  `RESOURCE_LINKAGE_ERROR` entry whose `motivation` literally says
  "See ETL logs for blocked resources" — without parsing the log the
  scale of the abort is invisible. The full expansion is written to
  `blocked-record-ids.txt` (one ID per line), plus the first 20
  blocked `recordId / class / linkedTo / missingDependencies` tuples
  are printed inline as a sample.
- **"ERROR-line reconciliation"** is a safety net: it compares the
  total `ERROR` line count from `etl-idea.log` against the sum of the
  categories above (faults + wrong-number-format + linkage + HTTP).
  Any remainder surfaces as "Unexplained ERROR lines" with a sample,
  so a new log shape can't slip through silently.

### Stage 2 — staging

- The table **"Aerospike vs Audit API"** should match exactly. If it
  doesn't, the audit endpoint is lying or the staging set got pruned.
- **"Per-resource reconciliation"** is the cross-layer check. For each
  resource (Patient, Diagnosis, SystemicTreatment, …) it reports:

  | Column | Meaning |
  |---|---|
  | `records` | Distinct resource instances the audit API saw (`recordId` grouped by the prefix of `coreVariable`, because `etl-records.json` is field-level). |
  | `errored` | Distinct resource instances that had **at least one** field-level error. |
  | `staging` | Row count in the matching staging table. |
  | `missing` | `records − staging` — rows the audit API saw that aren't in staging. |
  | `status`  | One of `clean` / `explained` / `SILENT_LOSS` / `DUPLICATES` / `no_table` (see below). |

  **Reading the `status` column:**

  - **`clean`** — every instance the audit API saw landed in staging.
    The normal healthy case.
  - **`explained`** — rows are missing but the missing count is `≤ errored`,
    i.e. every missing row has a corresponding error on file. This is
    the expected shape for resources with validation failures (the
    errors blocked staging, and the report knows about them).
  - **`SILENT_LOSS`** — **more rows are missing than had errors**. The
    pipeline dropped rows without recording an error. Investigate
    `etl-records-by-resource.txt`, `etl-errors-by-resource-instances.txt`,
    and the staging table. This flips the overall verdict to DEGRADED.
  - **`DUPLICATES`** — more rows in staging than the audit API saw
    (`missing < 0`). Almost always means staging wasn't truncated
    before the upload and carries over from a previous run.
  - **`no_table`** — no staging table matched the resource name. Add a
    `RESOURCE_TO_TABLE_OVERRIDE` entry, or confirm the resource
    doesn't have a dedicated staging table.
- **"By `propertyInError`"** tells you how many errors actually pin
  down a field — most IDEA4RC errors do **not**, they're class-level
  failures. The report calls that out explicitly.

### Stage 3 — OMOP ETL

- **Job status** should be `COMPLETED`. `FAILED` → check `omop-severe.txt`.
- **STEP_COUNT highlights** splits steps into:
  - **100% filtered** (read > 0, write = 0) — mapping gap; a domain
    the pipeline knows about but has no rules for.
  - **partial filter** (write > 0 **and** filter > 0) — some rows
    mapped, some intentionally dropped. 
  - **skip > 0** — the processor threw on specific rows. Look at
    `omop-skip-by-type.txt`.
  - **0 reads** — that CSV section wasn't in the upload at all.
- **Total skips** is split into "retry artifacts"
  (`DuplicateKeyException` — not real data loss) and "real losses"
  (everything else). Only the second number matters.

### Section 5 — "Referential errors in four layers"

This is the most subtle part of the report. A single bad foreign-key
reference in the CSV can show up in **all four** of:

- **Layer A** — Stage 1, per-row `RECORD_CONVERSION_ERROR`.
- **Layer B** — Stage 1, global `RESOURCE_LINKAGE_ERROR` (one audit
  entry represents many blocked rows; the `Blocked record_id list`
  line in `etl-idea.log` gives the expansion).
- **Layer C** — Stage 3, Spring Batch filter (intentional, not an error).
- **Layer D** — Stage 3, skip-on-error (usually NPE).

The counts are **not independent**. Always investigate A first; B, C,
and D typically shrink once A is fixed.

### Section 6 — handshake

`staging_patients == omop_persons` is the "hello world" of pipeline
health. A mismatch means Stage 3 dropped people, and the "real skip"
count should explain how many.

### Section 7 — Success criteria & follow-ups

The final section renders a single criteria table plus a bullet list
of any criteria that failed. The verdict is deterministic — there is
no prose judgement — and only the exit code encodes the tier (see
[Usage](#usage)).

**Code prefixes in the criteria table:**

- **`I*`** — hard invariants (pipeline integrity). Any `✗` here means
  the run is not trustworthy; exit code `2`. Examples:
  - `I1` OMOP job status `== COMPLETED`
  - `I2`/`I3` Aerospike ↔ Audit API record/error counts match
  - `I4` staging_patients == omop_persons (the handshake)
  - `I5` every OMOP step satisfies `read == write + filter + skip`
  - `I6` every Stage 1 `ERROR` line is explained (no unknown shapes)
  - `I7` every API `.error` code is in `{RECORD_CONVERSION_ERROR,
    RESOURCE_LINKAGE_ERROR}`
  - `I8` zero severe (non-skip) OMOP errors
  - `I9` Aerospike `stop-writes-count` / `stop-writes-size` all `0`
    (neither set refused writes)
  - `I10` CSV rows == Aerospike `ExcelRecord` objects — only evaluated
    when `CSV_FILE` is configured so the collector persists
    `csv-rows.txt`; skipped otherwise
- **`S*`** — soft invariants (data integrity). Any `✗` means the data
  landed with issues but the pipeline ran; exit code `1`. Examples:
  - `S1` pod health: every pod `Running`, `READY=N/N`, `0` restarts
  - `S2` no `SILENT_LOSS` row in the per-resource reconciliation
  - `S3` no `DUPLICATES` row
  - `S4` no 100 %-filter step targeting a core OMOP domain (target is
    parsed from the Spring Batch step name, e.g.
    `systemicTreatmentToProcedureStep` → `procedure_occurrence`)
  - `S5` zero non-retry OMOP skips
  - `S6` no linkage abort (`Unsaved resources == 0`)
  - `S7` no core OMOP domain with a count of `0` in `omop-counts.txt`
    (catches the case where every step targeting a domain filters out
    all rows)
- **`B*`** — budgets (tunable tolerances). Any `✗` means something
  exceeded the configured budget; exit code `1`. Thresholds live in
  `audit.conf` under `SUCCESS_BUDGET_*` / `SUCCESS_CORE_*`
  (see [Configuration](#every-key)):
  - `B1` wrong-number-format rows (absolute)
  - `B2` linkage-abort rate
  - `B3` CORE resource errored-rate
  - `B4` non-CORE resource errored-rate
  - `B5` OMOP real-skip rate

**Follow-ups** lists one bullet per failing criterion with the
measured value and the rule. If every criterion passes, the section
reads `No follow-ups — all criteria pass.`

To tighten or loosen the bar for a given deployment, edit the
`SUCCESS_*` keys in that deployment's `audit.conf`. Adding new
criteria requires a matching entry in `audit-summarize.sh` — see
[Adding a new check](#adding-a-new-check).

---

## Troubleshooting

### "No audit config file provided."

You didn't pass `-c`, didn't set `CAPSULE_AUDIT_CONFIG`, and there's no
`audit.conf` next to the script or in your current directory. Either
pass `-c` or `cp audit.conf.example audit.conf` in the script's
directory.

### "Missing required binary: microk8s.kubectl"

Your cluster doesn't use microk8s. Edit the config:

```bash
KUBECTL=kubectl
```

Or set it inline: `KUBECTL=kubectl ./audit-pipeline-errors.sh -c ...`.

### "etl-idea pod not found — skipping Stage 1."

Your pipeline uses different labels. Find the actual selector:

```bash
kubectl -n <namespace> get pods --show-labels
```

and update `ETL_APP_SELECTOR` (or the other selectors) in your config.

### "Could not reach <audit URL>. Probing alternative paths..."

The audit API is on a different path on your build. The script
automatically probes a few alternates and writes HTTP status codes to
`etl-errors.probe`. Pick the path that returned `200` and set:

```bash
AUDIT_ERRORS_PATH=/the/path/you/found
AUDIT_RECORDS_PATH=/the/records/path
```

### Audit API returns auth errors (401/403)

The script reaches the audit API through `kubectl port-forward`, which
talks directly to the pod/service and should bypass ingress-level
auth. If the pod itself enforces auth, you'll have to either disable
auth for in-cluster calls or extend the collector to supply a token.

### Empty / zero-line logs

Your cluster's log aggregation rotated the logs away. The collector
uses `--tail=500000` for etl-idea and `--tail=5000000` for omop-etl;
those limits are baked in at the top of the collector script if you
need to increase them.

### `jq: command not found`

Install `jq` (`apt install jq`, `brew install jq`, etc.). Without it,
the audit API dump is saved but the per-code / per-resource
breakdowns are skipped.

### `psql` or `asinfo` not on the pod's `PATH`

The collector runs `kubectl exec <pod> -- psql ...` directly, so the
binary has to be on the pod. This is the normal case for the official
Postgres and Aerospike images. If you've built custom images that
don't include them, either rebuild to include them or extend the
collector to `kubectl cp` a binary in first.

---

## Extending / porting to a new deployment

### New deployment, same pipeline shape

1. `cp audit.conf.example new-site.conf`
2. Edit `NS`, `KUBECTL`, and any selectors/usernames that differ.
3. Run `./audit-pipeline-errors.sh -c new-site.conf`.
4. If pods aren't found, look at the "Discover pods" section of the
   output — it prints the selector it used, so you can tell at a
   glance which key needs changing.

### Different pipeline shape

If one of the components (Aerospike, the audit API, one of the
Postgres instances) is missing or replaced:

- Set the relevant selector/pattern to something that matches nothing
  — the corresponding section of the collector will print a yellow
  "skipping" notice and move on rather than fail.
- Or comment out / delete those sections in the collector script. The
  summarizer already handles missing artifacts gracefully (it shows
  "No &lt;file&gt; — skipping" for each one).

### Adding a new check

1. Add a new section to `audit-pipeline-errors.sh` that writes a new
   artifact file into `$OUTDIR`.
2. Add a new block to `audit-summarize.sh` that reads that file and
   emits a report section.
3. Add a success criterion in `audit-summarize.sh`'s Section 7
   (`crit <CODE> <TIER> <pass|fail|skip> <desc> <rule> <measured>`).
   Rules of thumb:
   - Prefer **invariants** (`I*` hard, `S*` soft) when the check is
     binary.
   - Prefer **budgets** (`B*`) when the check is a rate or count that
     scales with input volume; expose the threshold as a new
     `SUCCESS_BUDGET_*` key in `audit-config-lib.sh`,
     `audit.conf.example`, and the README's
     [Every key](#every-key) table (the three lists are drift-checked
     in [Contributing](#contributing)).
   - Always give the criterion a stable code and cite the exact
     artifact file the measurement comes from.

Follow the existing pattern of graceful degradation: every read
should be guarded with `[[ -f "$DIR/new-artifact" ]]` so the
summarizer still works on old artifact bundles that predate your
check, and a criterion with a missing data source should `crit …
skip` (n/a) rather than fail.

---

## Design notes

### Why `set -u` and `set -o pipefail` but NOT `set -e`

The collector runs ~50 `kubectl`, `grep`, `awk`, and `psql` calls.
Many of them are expected to return non-zero when the thing they're
looking for is absent (empty `grep` match, pod not deployed, DB table
missing, …) — that's a signal, not an error, and the next section
should still run.

`set -u` and `set -o pipefail` catch the errors we actually care about
(unset variables, broken pipelines) without aborting on every
harmless `grep` miss. Per-call fallbacks use `|| true` where silence
is deliberate. Don't add `set -e` without also adding `|| true` to
every optional call.

### Why the code is structured in two separate scripts

- The collector is the only part that needs cluster credentials. The
  summarizer is pure file I/O — you can ship an artifact tarball to
  a colleague and they can re-summarize locally without any cluster
  access.
- The summarizer can be re-run as many times as you want (e.g. after
  tweaking a threshold) with zero impact on the cluster.
- It also lets you archive an artifact bundle and re-summarize it
  months later without having to stand up the original environment.

---

## Contributing

Issues and pull requests are welcome. Before opening a PR:

1. Run `bash -n` on each script to confirm syntax.
2. Run [`shellcheck`](https://www.shellcheck.net/) at `-S info -x`;
   the codebase is currently clean at that level.
3. If you're touching `audit-config-lib.sh`, keep bash 3.2 compatible
   (no `declare -A`, `mapfile`, `${var,,}`, etc. — macOS ships bash
   3.2 by default).
4. Never commit anything under `$OUTDIR`. The shipped `.gitignore`
   should catch the default path, but double-check before pushing.
5. Keep config keys in sync across `audit-config-lib.sh` (defaults),
   `audit.conf.example` (template), and this README (documentation
   table). A quick way to audit drift:
   ```bash
   # All three lists should be identical.
   grep -oE '\$\{[A-Z_]+:=' audit-config-lib.sh  | sort -u
   grep -E  '^[A-Z_]+='      audit.conf.example  | sort -u
   grep -oE '\| `[A-Z_]+`'   README.md           | sort -u
   ```

## Security

If you believe you've found a security issue (for example, a path
that leaks credentials into an artifact, or a shell-injection vector
through a config value), **please do not open a public issue**.
Instead, open a private security advisory on the repository (GitHub
→ Security → Advisories → Report a vulnerability) or contact the
maintainer directly.

Routine hardening reminders for users:

- Store `audit.conf` with file mode `0600` — it contains Kubernetes
  namespace and DB user names that may be sensitive.
- Treat `$OUTDIR` as PHI / PII; keep it off shared filesystems.
- Scripts never write credentials to disk, but they do source the
  config file as bash — don't point `-c` at a file you don't trust.

## License

MIT — see [`LICENSE`](LICENSE). Copyright (c) 2026 nara.
