#!/usr/bin/env python3
"""
Builds the "OHDSI Mapped Cohort" HTML dashboard from a
basic_mapped_data_stats.py --extended-queries --output-csv-files run. Source-
agnostic -- the masthead's "Source" label comes from cdm_source.csv, not from
any assumption about where the data came from (Synthea or otherwise).

Usage:
    python generate_cohort_dashboard.py \\
        --stats-dir /data/ohdsi/stats/20260915_223756 \\
        --output /data/ohdsi/stats/20260915_223756/cohort_dashboard.html

Reads only the stats CSVs (the host running this has no pyarrow/pandas/duckdb) and
injects a single JSON data blob into cohort_dashboard_template.html, which sits
alongside this script. All rendering/interaction logic lives in the template's JS --
this script's job is purely to compute the same aggregates a careful reviewer would
compute by hand: top-N by n / n_r, treemap "top 10 + other" buckets, and the
numeric-only top-20 measurement list used for the value-distribution box plots.
"""

import argparse
import csv
import json
import pathlib
import re
import subprocess
import sys
from datetime import datetime


HERE = pathlib.Path(__file__).resolve().parent
DEFAULT_TEMPLATE = HERE / "cohort_dashboard_template.html"
PLACEHOLDER = "/*__COHORT_DATA_JSON__*/ null"

# Hand-curated display shortenings for verbose OMOP concept names. Anything not
# listed here falls back to the raw concept_name -- the treemap already truncates
# long labels client-side to fit, so an unlisted name degrades gracefully, it just
# won't be as pretty. Extend this as new concepts show up in future runs.
SHORT_NAMES = {
    "Systolic blood pressure": "Systolic BP",
    "Diastolic blood pressure": "Diastolic BP",
    "Pain severity - 0-10 verbal numeric rating [Score] - Reported": "Pain severity (0–10, verbal)",
    "Hemoglobin A1c/Hemoglobin.total in Blood": "Hemoglobin A1c",
    "Triglyceride [Mass/volume] in Serum or Plasma": "Triglycerides",
    "Cholesterol [Mass/volume] in Serum or Plasma": "Total cholesterol",
    "Cholesterol in LDL [Mass/volume] in Serum or Plasma by Direct assay": "LDL cholesterol",
    "Cholesterol in HDL [Mass/volume] in Serum or Plasma": "HDL cholesterol",
    "Fasting glucose [Mass/volume] in Serum or Plasma": "Fasting glucose",
    "Weight difference [Mass difference] --pre dialysis - post dialysis": "Weight diff., dialysis",
    "Glomerular filtration rate, Creatinine-based formula (MDRD)/1.73 sq M": "GFR (MDRD)",
    "Glomerular filtration rate [Volume Rate/Area] in Serum, Plasma or Blood by "
    "Creatinine-based formula (MDRD)/1.73 sq M": "GFR (MDRD)",
    "Specific gravity of Urine by Test strip": "Specific gravity, urine",
    "Ketones [Presence] in Urine by Test strip": "Ketones, urine",
    "Hemoglobin [Presence] in Urine by Test strip": "Hemoglobin, urine",
    "Leukocyte esterase [Presence] in Urine by Test strip": "Leukocyte esterase, urine",
    "1 ML epoetin alfa 4000 UNT/ML Injection [Epogen]": "Epoetin alfa 4000 UNT/ML Inj.",
    "insulin lispro 100 UNT/ML Injectable Solution [Humalog]": "Insulin lispro [Humalog]",
    "24 HR metformin hydrochloride 500 MG Extended Release Oral Tablet": "Metformin ER 500 MG",
    "Continuous positive airway pressure/Bilevel positive airway pressure mask": "CPAP/BPAP mask",
    "American Indian or Alaska Native": "Amer. Indian / Alaska Native",
    "Race": "Race [PRAPARE]",
    "Are you worried about losing your housing [PRAPARE]": "Worried about losing housing [PRAPARE]",
    "Has season or migrant farm work been your or your family's main source of "
    "income at any point in past 2 years [PRAPARE]": "Seasonal/migrant farm work income [PRAPARE]",
    "How often do you see or talk to people that you care about and feel close to "
    "[PRAPARE]": "Feel close to people [PRAPARE]",
    "Are you a refugee": "Refugee status",
    "Body mass index (BMI) [Ratio]": "BMI",
}

# UCUM unit codes as stored in unit_concept_code, mapped to the friendlier form used
# for display. Anything not listed here is shown as-is (most UCUM codes, e.g.
# "mg/dL", "kg", "cm", "%", "/min", already read fine to a clinical audience).
UNIT_DISPLAY = {
    "mm[Hg]": "mmHg",
    "kg/m2": "kg/m²",
}


def short_name(name):
    return SHORT_NAMES.get(name, name)


def display_unit(unit_code):
    return UNIT_DISPLAY.get(unit_code, unit_code)


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def read_single(path):
    rows = read_csv(path)
    if not rows:
        raise ValueError(f"{path} has no data rows")
    return rows[0]


def as_int(v):
    return int(round(float(v)))


def as_float(v, ndigits=3):
    return round(float(v), ndigits)


def top_n_by(rows, key, n):
    return sorted(rows, key=lambda r: -as_int(r[key]))[:n]


def suppress_and_merge(rows, n_col, label_fn, threshold):
    """Small-cell suppression for a categorical breakdown (gender/race/ethnicity/visit
    type/...). Any row whose distinct-patient count is below `threshold` is pulled out
    and folded into a single aggregate "Suppressed" bucket instead of ever displaying
    its exact (small) count -- that bucket's total is itself only shown if it clears
    the same threshold, so a lone small group can't be reverse-engineered from it.
    threshold <= 0 disables suppression entirely (fine for fully synthetic data; never
    do this against real patient data without an explicit, deliberate choice to do so).
    This is basic single-release cell suppression, not full statistical disclosure
    control -- it does not protect against differencing attacks across multiple
    dashboard releases from overlapping cohorts. Have your IRB/DUA owner confirm the
    threshold and this approach before relying on it for a real-data report.
    """
    if threshold <= 0:
        return [(label_fn(r), as_int(r[n_col])) for r in rows]
    kept, suppressed_n, suppressed_groups = [], 0, 0
    for r in rows:
        n = as_int(r[n_col])
        if n < threshold:
            suppressed_n += n
            suppressed_groups += 1
        else:
            kept.append((label_fn(r), n))
    if suppressed_groups and suppressed_n >= threshold:
        label = f"Suppressed (n<{threshold}, {suppressed_groups} group{'s' if suppressed_groups != 1 else ''})"
        kept.append((label, suppressed_n))
    elif suppressed_groups:
        # even the merged bucket doesn't clear the threshold -- fold it into the
        # largest kept group rather than displaying a still-small aggregate count
        label = "Suppressed (folded into largest group)"
        if kept:
            kept.sort(key=lambda p: -p[1])
            biggest_label, biggest_n = kept[0]
            kept[0] = (biggest_label + f" (incl. {suppressed_groups} suppressed cell{'s' if suppressed_groups != 1 else ''})",
                       biggest_n + suppressed_n)
        else:
            kept.append((label, suppressed_n))
    return kept


NEUTRAL_LABEL_PREFIXES = ("no matching concept", "suppressed")


DEFAULT_PALETTE = (1, 2, 3, 4, 5, 6, 7, 8)
# Visit type historically used this non-sequential order (s7 in the 4th slot rather
# than s4) so "Non-hospital institution Visit" / "Telehealth" / "Home Visit" -- the
# three concepts that needed custom mapping fixes -- read as visually distinct from
# the three high-volume built-in visit types rather than just continuing the ramp.
VISIT_TYPE_PALETTE = (1, 2, 3, 7, 4, 5, 6, 8)


def colorize(pairs, palette=DEFAULT_PALETTE):
    """pairs: [(label, value), ...] from suppress_and_merge. Sorts desc by value and
    assigns palette colors in rank order (cycling through `palette`, an ordering of
    s1..s8 slots), except residual/no-info buckets ('No matching concept', a
    suppressed-cell bucket), which always render neutral (ink-mute) rather than
    taking a palette slot."""
    pairs_sorted = sorted(pairs, key=lambda p: -p[1])
    out, ci = [], 0
    for label, value in pairs_sorted:
        if label.lower().startswith(NEUTRAL_LABEL_PREFIXES):
            color = "var(--ink-mute)"
        else:
            color = color_s(palette[ci % len(palette)])
            ci += 1
        out.append({"label": label, "value": value, "color": color})
    return out


def colorize_gender(pairs):
    """Gender gets a fixed semantic mapping (Female always blue/s1, Male always
    orange/s2) rather than rank-based colors, since which one is larger varies by
    site and swapping the color each run would be a needless inconsistency. Any
    other label (a third gender category, or a suppressed bucket) falls back to
    rank-based colors from the remaining palette slots."""
    pairs_sorted = sorted(pairs, key=lambda p: -p[1])
    fixed = {"female": color_s(1), "male": color_s(2)}
    out, ci = [], 0
    fallback_palette = tuple(i for i in DEFAULT_PALETTE if i not in (1, 2))
    for label, value in pairs_sorted:
        low = label.lower()
        if low.startswith(NEUTRAL_LABEL_PREFIXES):
            color = "var(--ink-mute)"
        elif low in fixed:
            color = fixed[low]
        else:
            color = color_s(fallback_palette[ci % len(fallback_palette)])
            ci += 1
        out.append({"label": label, "value": value, "color": color})
    return out


def year_series(rows, year_col, n_col, years):
    by_year = {as_int(r[year_col]): as_int(r[n_col]) for r in rows if r[year_col].strip() != ""}
    return [by_year.get(y, 0) for y in years]


def color_s(i):
    return "var(--s%d)" % i


def derive_source_label(cdm_source):
    """The masthead 'Source' field describes whatever dataset this run was built
    from -- it must never assume Synthea specifically, since the same script runs
    against real (non-synthetic) data too. Pull it from cdm_source.csv, which the
    ETL populates for every run regardless of source, rather than hardcoding
    anything dataset-specific here."""
    name = cdm_source.get("cdm_source_name", "").strip()
    holder = cdm_source.get("cdm_holder", "").strip()
    abbrev = cdm_source.get("cdm_source_abbreviation", "").strip()
    if name and holder and holder.lower() not in ("", "not specified"):
        return f"{name} ({holder})"
    if name:
        return name
    if abbrev:
        return abbrev
    return "unspecified source"


def build_data(stats_dir, hash_id, source_label, min_cell_size=0):
    d = pathlib.Path(stats_dir)

    def p(name):
        return d / f"{name}.csv"

    count_people = read_single(p("count_people"))
    count_visits = read_single(p("count_visits"))
    count_deaths = read_single(p("count_deaths"))
    count_obs_periods = read_single(p("count_observation_periods"))
    care_sites = read_single(p("care_site_count"))
    providers = read_single(p("provider_count"))
    locations = read_single(p("locations_count"))
    cdm_source = read_single(p("cdm_source"))

    if source_label is None:
        source_label = derive_source_label(cdm_source)

    patients = as_int(count_people["n"])
    visits = as_int(count_visits["n_r"])
    deaths = as_int(count_deaths["n"])
    obs_periods = as_int(count_obs_periods["n"])
    n_care_sites = as_int(care_sites["n_r"])
    n_providers = as_int(providers["n_r"])
    n_locations = as_int(locations["n_r"])

    if min_cell_size > 0 and patients < min_cell_size:
        raise ValueError(
            f"Cohort size ({patients}) is below the minimum cell size ({min_cell_size}) -- "
            f"refusing to generate a report for a cohort this small."
        )
    deaths_suppressed = min_cell_size > 0 and 0 < deaths < min_cell_size

    # year span: earliest/latest visit year present in the full (unfiltered) yearly counts
    yearly_visit_rows = read_csv(p("yearly_visit_counts"))
    all_visit_years = [as_int(r["visit_year"]) for r in yearly_visit_rows if r["visit_year"].strip() != ""]
    year_span = [min(all_visit_years), max(all_visit_years)]

    # 15-year rolling window ending at the latest year present, for the temporal-coverage charts
    max_year = max(all_visit_years)
    years = list(range(max_year - 14, max_year + 1))

    gender_rows = read_csv(p("count_gender"))
    race_rows = read_csv(p("count_race"))
    ethnicity_rows = read_csv(p("count_ethnicity"))
    visit_type_rows = read_csv(p("count_visit_concepts_count"))
    unmapped_visit_types = read_csv(p("unmapped_visit_types"))
    drugs_not_mapped_std = read_csv(p("drugs_not_mapped_to_standard_concepts"))
    drugs_not_mapped_concept = read_csv(p("drug_not_mapped_to_concept_ids"))

    demographics = {
        "gender": colorize_gender(suppress_and_merge(
            gender_rows, "n", lambda r: r["gender_concept_name"].title(), min_cell_size)),
        "race": colorize(suppress_and_merge(
            race_rows, "n", lambda r: short_name(r["race_concept_name"]), min_cell_size)),
        "ethnicity": colorize(suppress_and_merge(
            ethnicity_rows, "n", lambda r: r["ethnicity_concept_name"], min_cell_size)),
    }

    visit_type = colorize(suppress_and_merge(
        visit_type_rows, "n", lambda r: r["visit_concept_name"], min_cell_size), palette=VISIT_TYPE_PALETTE)
    mapped_visit_labels = {"Non-hospital institution Visit", "Telehealth", "Home Visit"}
    for entry in visit_type:
        if entry["label"] in mapped_visit_labels:
            entry["tag"] = "MAPPED"

    data_quality = []
    if not unmapped_visit_types:
        data_quality.append({
            "text": "Visit type mapping &mdash; 0 unmapped encounter classes",
            "detail": "every visit_source_value resolves to a standard visit concept "
                      "&mdash; 0 rows with visit_concept_id = 0",
        })
    else:
        n_unmapped = sum(as_int(r["n_r"]) for r in unmapped_visit_types)
        data_quality.append({
            "text": f"Visit type mapping &mdash; {len(unmapped_visit_types)} unmapped encounter class(es)",
            "detail": f"{n_unmapped:,} rows with visit_concept_id = 0 &mdash; see unmapped_visit_types.csv",
        })

    if not drugs_not_mapped_std and not drugs_not_mapped_concept:
        data_quality.append({
            "text": "Drug mapping &mdash; 0 source concepts unresolved",
            "detail": "drug_exposure: all drug_source_concept_id values resolve to a standard concept",
        })
    else:
        n_unresolved = (
            sum(as_int(r["n_r"]) for r in drugs_not_mapped_std)
            + sum(as_int(r["n_r"]) for r in drugs_not_mapped_concept)
        )
        data_quality.append({
            "text": "Drug mapping &mdash; unresolved source concepts found",
            "detail": f"{n_unresolved:,} rows unresolved &mdash; see drugs_not_mapped_to_standard_concepts.csv "
                      f"/ drug_not_mapped_to_concept_ids.csv",
        })

    if obs_periods == patients:
        data_quality.append({
            "text": "Person &middot; observation period coverage &mdash; 1:1",
            "detail": f"every one of the {patients:,} patients has exactly one observation_period row",
        })
    else:
        data_quality.append({
            "text": "Person &middot; observation period coverage &mdash; NOT 1:1",
            "detail": f"{obs_periods:,} observation periods for {patients:,} patients "
                      f"&mdash; investigate observation_period generation",
        })

    # ---------------- top concepts (top 6 by n, ties broken by n_r) ----------------
    # Concepts with fewer than min_cell_size patients are excluded from the candidate
    # pool entirely -- a rare concept never gets a labeled bar showing its small n,
    # it just doesn't appear (no aggregate "other" needed here; this list is a top-6
    # highlight, not an accounting of the whole domain the way the treemaps are).
    def top6_by_n(rows, name_col):
        eligible = [r for r in rows if as_int(r["n"]) >= min_cell_size] if min_cell_size > 0 else rows
        rows_sorted = sorted(eligible, key=lambda r: (-as_int(r["n"]), -as_int(r["n_r"])))
        return [
            {"label": short_name(r[name_col]), "value": as_int(r["n"]), "color": color_s(1)}
            for r in rows_sorted[:6]
        ]

    condition_concepts = read_csv(p("condition_concepts_count"))
    procedure_concepts = read_csv(p("procedure_concepts_count"))
    drug_concepts = read_csv(p("drug_concepts_count"))
    measurement_concepts = read_csv(p("measurement_concepts_count"))
    observation_concepts = read_csv(p("observation_concepts_count"))
    device_concepts = read_csv(p("device_concepts_count"))

    top_concepts = {
        "condition": top6_by_n(condition_concepts, "condition_concept_name"),
        "drug": top6_by_n(drug_concepts, "concept_name"),
        "procedure": top6_by_n(procedure_concepts, "procedure_concept_name"),
        "measurement": top6_by_n(measurement_concepts, "measurement_concept_name"),
    }

    # ---------------- temporal coverage ----------------
    domain_specs = [
        ("visit", "Visits", "s1", p("yearly_visit_counts"), "visit_year"),
        ("condition", "Conditions", "s2", p("yearly_condition_counts"), "condition_year"),
        ("procedure", "Procedures", "s3", p("yearly_procedure_counts"), "procedure_year"),
        ("drug", "Drugs", "s4", p("yearly_drug_counts"), "drug_year"),
        ("measurement", "Measurements", "s5", p("yearly_measurement_counts"), "measurement_year"),
        ("observation", "Observations", "s7", p("yearly_observation_counts"), "observation_year"),
        ("device", "Devices", "s8", p("yearly_device_counts"), "device_year"),
    ]
    yearly_domains = []
    for key, title, color, path, year_col in domain_specs:
        rows = read_csv(path)
        yearly_domains.append({
            "key": key, "title": title, "color": color,
            "data": year_series(rows, year_col, "n", years),
        })

    # ---------------- treemaps: top 10 by n_r + "other" bucket per table ----------------
    # Concepts with fewer than min_cell_size patients are excluded from top-10
    # candidacy (their rows still count toward total_n_r, so they're absorbed into
    # the "other" bucket automatically -- their individual small n/n_r is never shown).
    def build_treemap(key, title, table, concepts_rows, name_col, total_n_r):
        eligible = [r for r in concepts_rows if as_int(r["n"]) >= min_cell_size] if min_cell_size > 0 else concepts_rows
        top10 = top_n_by(eligible, "n_r", 10)
        items = [
            {"name": short_name(r[name_col]), "n": as_int(r["n"]), "n_r": as_int(r["n_r"])}
            for r in top10
        ]
        other_n_r = total_n_r - sum(it["n_r"] for it in items)
        return {
            "key": key, "title": title, "table": table,
            "total_n_r": total_n_r, "other_n_r": other_n_r,
            "concept_count": len(concepts_rows),
            "items": items,
        }

    count_conditions = read_single(p("count_conditions"))
    count_procedures = read_single(p("count_procedures"))
    count_drugs = read_single(p("count_drugs"))
    count_measurements = read_single(p("count_measurements"))
    count_observations = read_single(p("count_observations"))
    count_devices = read_single(p("count_devices"))

    treemaps = [
        build_treemap("condition", "Condition", "condition_occurrence", condition_concepts,
                       "condition_concept_name", as_int(count_conditions["n_r"])),
        build_treemap("procedure", "Procedure", "procedure_occurrence", procedure_concepts,
                       "procedure_concept_name", as_int(count_procedures["n_r"])),
        build_treemap("drug", "Drug exposure", "drug_exposure", drug_concepts,
                       "concept_name", as_int(count_drugs["n_r"])),
        build_treemap("measurement", "Measurement", "measurement", measurement_concepts,
                       "measurement_concept_name", as_int(count_measurements["n_r"])),
        build_treemap("observation", "Observation", "observation", observation_concepts,
                       "concept_name", as_int(count_observations["n_r"])),
        build_treemap("device", "Device exposure", "device_exposure", device_concepts,
                       "concept_name", as_int(count_devices["n_r"])),
    ]

    for tm in treemaps:
        computed_total = sum(it["n_r"] for it in tm["items"]) + tm["other_n_r"]
        assert computed_total == tm["total_n_r"], (
            f"treemap '{tm['key']}' items+other ({computed_total}) != total_n_r ({tm['total_n_r']})"
        )
        if tm["other_n_r"] < 0:
            raise ValueError(
                f"treemap '{tm['key']}': other_n_r is negative ({tm['other_n_r']}) -- the domain's "
                f"count_* total is smaller than the sum of its own top 10 concepts; check the source CSVs"
            )

    # ---------------- measurement value distributions: top 20 numeric-only, by n_r ----------------
    numeric_measurements = [
        r for r in measurement_concepts
        if r["min_value_as_number"].strip() != "" and r["p50"].strip() != ""
    ]
    if min_cell_size > 0:
        # a measurement's exact min/max are themselves potentially identifying (an
        # extreme outlier value can correspond to a single patient), independent of
        # the small-cell-count concern this threshold otherwise covers -- dropping
        # sub-threshold-n measurements here does NOT address that separate risk.
        numeric_measurements = [r for r in numeric_measurements if as_int(r["n"]) >= min_cell_size]
    top20 = top_n_by(numeric_measurements, "n_r", 20)
    measurements = []
    for r in top20:
        concept_name_lower = r["measurement_concept_name"].lower()
        unit = r["unit_concept_code"].strip()
        if unit in ("", "No matching concept"):
            if "pain severity" in concept_name_lower:
                unit = "score"
            elif "[presence]" in concept_name_lower:
                unit = "presence (0/1)"
            elif "specific gravity" in concept_name_lower:
                unit = "ratio"
            else:
                unit = "mixed units"
        else:
            unit = display_unit(unit)
        measurements.append({
            "name": short_name(r["measurement_concept_name"]),
            "full": r["measurement_concept_name"],
            "unit": unit,
            "n": as_int(r["n"]), "n_r": as_int(r["n_r"]),
            "min": as_float(r["min_value_as_number"]), "p05": as_float(r["p05"]),
            "p25": as_float(r["p25"]), "p50": as_float(r["p50"]),
            "mean": as_float(r["mean_value_as_number"]),
            "p75": as_float(r["p75"]), "p95": as_float(r["p95"]),
            "max": as_float(r["max_value_as_number"]),
        })

    # ---------------- metadata ----------------
    stats_run_tag = d.name
    stats_run_label = stats_run_tag
    m = re.match(r"^(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})$", stats_run_tag)
    if m:
        dt = datetime(*[int(x) for x in m.groups()])
        stats_run_label = (
            dt.strftime("%Y") + "‑" + dt.strftime("%m") + "‑" + dt.strftime("%d")
            + " " + dt.strftime("%H:%M:%S") + " UTC"
        )

    vocab = cdm_source.get("vocabulary_version", "").strip()
    vocab = vocab.replace("-", "‑") if vocab else "unknown"

    def format_date_label(raw):
        raw = (raw or "").strip()
        if not raw:
            return ""
        try:
            dt = datetime.strptime(raw[:10], "%Y-%m-%d")
        except ValueError:
            return raw.replace("-", "‑")
        return dt.strftime("%Y") + "‑" + dt.strftime("%m") + "‑" + dt.strftime("%d")

    source_release_date = format_date_label(cdm_source.get("source_release_date"))
    cdm_release_date = format_date_label(cdm_source.get("cdm_release_date"))

    footer_note = ("mean/stddev computed directly in the SQL (measurement_concepts_count / "
                   "measurement_units_ranges), no separate duckdb pass needed")
    if min_cell_size > 0:
        footer_note += f" · small-cell suppression applied: counts below n={min_cell_size} are merged/redacted"

    return {
        "meta": {
            "sourceLabel": source_label,
            "etl": "PreparedSource2OHDSI",
            "statsRunLabel": stats_run_label,
            "statsRunTag": stats_run_tag,
            "vocabulary": vocab,
            "sourceReleaseDate": source_release_date,
            "cdmReleaseDate": cdm_release_date,
            "hashId": hash_id,
            "footerNote": footer_note,
            "minCellSize": min_cell_size,
        },
        "tiles": {
            "patients": patients,
            "yearSpan": year_span,
            "visits": visits,
            "deaths": deaths,
            "deathsSuppressed": deaths_suppressed,
            "observationPeriods": obs_periods,
            "careSites": n_care_sites,
            "providers": n_providers,
            "locations": n_locations,
        },
        "demographics": demographics,
        "visitType": visit_type,
        "dataQuality": data_quality,
        "topConcepts": top_concepts,
        "yearly": {"years": years, "domains": yearly_domains},
        "treemaps": treemaps,
        "measurements": measurements,
    }


def git_short_hash(repo_dir):
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=repo_dir, capture_output=True, text=True, timeout=5, check=True,
        )
        return out.stdout.strip()
    except Exception:
        return "unknown"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--stats-dir", required=True,
        help="A basic_mapped_data_stats.py --extended-queries --output-csv-files timestamped output directory",
    )
    ap.add_argument("--output", required=True, help="Path to write the generated HTML report")
    ap.add_argument(
        "--template", default=str(DEFAULT_TEMPLATE),
        help="Path to the HTML template (default: cohort_dashboard_template.html next to this script)",
    )
    ap.add_argument(
        "--hash-id", default=None,
        help="Short commit hash to display in the footer (default: current HEAD of this script's repo)",
    )
    ap.add_argument(
        "--source-label", default=None,
        help="Masthead 'Source' label. Default: derived from cdm_source.csv's cdm_source_name "
             "(and cdm_holder, if set) in --stats-dir -- pass this to override with something more "
             "descriptive, e.g. '--source-label \"Synthea (synthetic)\"' for a dev/test run.",
    )
    ap.add_argument(
        "--min-cell-size", type=int, default=0, metavar="N",
        help="Small-cell suppression threshold: any demographic bucket, concept, or measurement "
             "with fewer than N distinct patients is merged/redacted rather than shown. Default 0 "
             "(disabled) -- fine for fully synthetic data (e.g. Synthea); pick a value with your "
             "IRB/DUA owner (11 and 5 are common conventions) before running against real patient "
             "data. This is basic single-release cell suppression, not full statistical disclosure "
             "control.",
    )
    args = ap.parse_args()

    if args.min_cell_size <= 0:
        print(
            "warning: --min-cell-size is 0 (no small-cell suppression) -- do not run this against "
            "real/PHI data without setting it; see --help.",
            file=sys.stderr,
        )

    hash_id = args.hash_id or git_short_hash(HERE)

    data = build_data(args.stats_dir, hash_id, args.source_label, min_cell_size=args.min_cell_size)

    template_path = pathlib.Path(args.template)
    template_html = template_path.read_text(encoding="utf-8")
    if PLACEHOLDER not in template_html:
        print(f"error: placeholder not found in template {template_path}", file=sys.stderr)
        sys.exit(1)

    # json.dumps doesn't escape "</" by default; a concept name containing "</script>"
    # would otherwise prematurely close the tag this gets embedded in.
    json_blob = json.dumps(data).replace("</", "<\\/")
    output_html = template_html.replace(PLACEHOLDER, json_blob)
    out_path = pathlib.Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(output_html, encoding="utf-8")
    print(f"Wrote {out_path} ({len(output_html):,} bytes)")


if __name__ == "__main__":
    main()
