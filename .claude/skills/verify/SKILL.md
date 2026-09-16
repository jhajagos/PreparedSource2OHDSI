---
name: verify
description: How to build, run, and verify the Synthea-to-OHDSI pipeline end-to-end in the podman container. Use whenever a change touches map/prepared_source/synthea or map/ohdsi and needs to be observed running, not just unit-tested.
---

# Verifying pipeline changes (podman container)

This repo has no unit-testable "surface" for mapping/CSV changes — the real
surface is the Spark ETL pipeline's output (OHDSI CDM parquet + data-quality
stats CSVs). Verify by running the pipeline and reading its output, not by
reading the mapping code.

## Environment

- Container: `synthea-ohdsi` (image `localhost/synthea-ohdsi:latest`), built from
  `map/prepared_source/synthea/docker/Dockerfile`.
- The container has its **own baked-in copy** of this repo at
  `/root/github/PreparedSource2OHDSI` — host edits do NOT propagate automatically.
  After editing a mapping file on the host, copy it in:
  ```
  podman cp <host path> synthea-ohdsi:/root/github/PreparedSource2OHDSI/<same relative path>
  ```
  (`map_synthea_to_prepared_source.py` and its `mappings/*.csv` are read directly
  from this path at runtime, not from the installed wheel.)
- Mounted volumes (bind mounts to `~/synthea/` on host, so output is inspectable
  from the host without `podman exec`):
  - `~/synthea/ohdsi/output` -> `/data/ohdsi/output` (final OHDSI CDM parquet)
  - `~/synthea/ohdsi/vocabulary` -> `/data/ohdsi/vocabulary/export` (OMOP vocab parquet)
  - `~/synthea/ps` -> `/data/preparedsource` (intermediate prepared_source parquet)
  - `~/synthea/stats` -> `/data/ohdsi/stats` (data-quality stats CSVs)
- Start it if stopped: `podman start synthea-ohdsi` (it's a long-lived named
  container, not `--rm` — reuse it, don't recreate).

## Gotcha: conda in non-interactive `podman exec`

`podman exec synthea-ohdsi bash -c "source /root/.bashrc && conda activate PySpark && ..."`
**fails silently-ish** (`conda: command not found`, exit 127) because `.bashrc`
returns early for non-interactive shells before reaching the conda-init block.
Bypass `.bashrc` entirely:
```
podman exec synthea-ohdsi bash -c "source /root/miniconda3/etc/profile.d/conda.sh && conda activate PySpark && <command>"
```
Always check the real exit code from the *inner* command, not the wrapper —
`echo EXIT_CODE=$?` after a `... > log 2>&1` only reflects the last command in
the chain; put it right after the command you care about.

## Running the full pipeline

```
podman exec synthea-ohdsi bash -c "source /root/miniconda3/etc/profile.d/conda.sh && conda activate PySpark && cd /root/scripts && ./map_synthea_csv_to_ohdsi_parquet.sh > rerun.log 2>&1; echo EXIT_CODE=\$?"
```
Runs (in order): `map_synthea_csv_to_prepared_source.sh` (Synthea CSV ->
prepared_source parquet) -> `map_prepared_source_to_ohdsi.sh` (-> OHDSI CDM
parquet) -> `basic_mapped_data_stats.py -l` for a quick sanity printout.
Takes several minutes (thousands of small Spark stages on this box) — run it
with `run_in_background: true` and wait for the completion notification
rather than polling.

**Gotcha: the wrapper script's stats invocation writes nothing to disk.**
`basic_mapped_data_stats.py -l` alone only prints basic (non-extended) query
results to stdout — `--output-csv-files` and `--extended-queries` both default
to `False`, and `unmapped_visit_types` (along with most of the other useful
data-quality checks) lives only in `extended_queries_to_run`, not the basic
set. To actually get a fresh `stats/<timestamp>/` directory with the checks
that matter, run it again manually with the right flags (also: use the
**default** `-c` config path, not the pipeline's
`prepared_source_to_ohdsi_config.json` — passing that one raises
`TypeError: string indices must be integers` deep in `attach_catalog_dict`):
```
podman exec synthea-ohdsi bash -c "source /root/miniconda3/etc/profile.d/conda.sh && conda activate PySpark && cd /root/scripts && python basic_mapped_data_stats.py -l --extended-queries --output-csv-files --output-base-directory /data/ohdsi/stats > stats_rerun.log 2>&1; echo EXIT_CODE=\$?"
```
This drops a new `~/synthea/stats/<YYYYMMDD_HHMMSS>/*.csv` set on the host.

## What to check after a run

- `unmapped_visit_types.csv` — encounter classes with `visit_concept_id = 0`.
  Empty (header only) = every `ENCOUNTERCLASS` in the source data resolved.
- `count_visit_concepts_count.csv` — cross-check `n`/`n_r` for the concept a
  fix targeted against the `n`/`n_r` it had in `unmapped_visit_types.csv`
  *before* the fix — they should reconcile exactly (rows don't get created or
  dropped, only re-labeled).
- `drugs_not_mapped_to_standard_concepts.csv` / `drug_not_mapped_to_concept_ids.csv`
  — same idea for drugs; should stay empty unless the change touched drug mapping.
- `count_people.csv` / `count_visits.csv` — compare against the prior stats run
  to confirm total row/person counts didn't shift (a mapping fix should never
  change cardinality, only concept_id values).
- To find the OMOP standard concept_code for a new mapping row, query the
  vocabulary directly rather than guessing — pyarrow/pandas aren't available
  on the host or in the base container python, but the `PySpark` conda env
  has PySpark, which can read the mounted vocabulary parquet directly:
  ```
  podman exec synthea-ohdsi /root/miniconda3/envs/PySpark/bin/python3 -c "
  from pyspark.sql import SparkSession
  spark = SparkSession.builder.master('local[2]').getOrCreate()
  spark.read.parquet('/data/ohdsi/vocabulary/export/concept.parquet').createOrReplaceTempView('concept')
  spark.sql(\"select concept_id, concept_code, concept_name from concept where vocabulary_id='Visit' and domain_id='Visit' order by concept_code\").show(50, truncate=False)
  "
  ```

## Generating the cohort dashboard

`map/ohdsi/utilities/generate_cohort_dashboard.py` turns a `--extended-queries
--output-csv-files` stats run into the "Synthea -> OHDSI Mapped Cohort" HTML
report (`cohort_dashboard_template.html`, same directory). Stdlib only (no
pandas/duckdb), so it runs on the host directly against the CSVs, no container
needed:
```
python3 map/ohdsi/utilities/generate_cohort_dashboard.py \
  --stats-dir ~/synthea/stats/<timestamp> \
  --output ~/synthea/stats/<timestamp>/cohort_dashboard.html
```

**`--min-cell-size` is a required judgment call, not a default you can skip.**
It defaults to `0` (suppression off) and prints a loud stderr warning at that
setting — fine for Synthea's fully synthetic output, since there's no
re-identification risk to suppress. The moment `--stats-dir` points at a run
built from **real** patient data, small patient counts next to a rare
condition/drug/demographic bucket become a genuine small-cell disclosure risk
(HIPAA Safe Harbor–style; 11 and 5 are common thresholds, but the actual
number is a policy call for whoever owns the IRB/DUA for that dataset, not
something to guess at here). Set `--min-cell-size N` explicitly before
generating a report from real data — it merges/redacts demographic buckets,
excludes sub-threshold concepts from the top-6/top-10/top-20 lists, masks the
deaths tile, and refuses to build anything at all if the whole cohort is
under the threshold. It is *not* full statistical disclosure control (no
protection against differencing attacks across overlapping-cohort releases,
and a measurement's exact min/max can still be a single patient's outlier
value even when `n` clears the threshold) — see `--help` and the
`suppress_and_merge()` docstring for what it does and doesn't cover.

Real-data output is a local file, full stop — do not publish it to a
hosted/cloud artifact viewer (Claude Artifacts or otherwise) unless that
hosting is explicitly covered by the dataset's data use agreement.
