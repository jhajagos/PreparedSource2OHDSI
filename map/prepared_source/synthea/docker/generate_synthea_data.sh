#!/usr/bin/env bash
#
# Unified replacement for generate_synthea_all_data.sh and generate_synthea_module_data.sh.
# Wraps `java -jar synthea-with-dependencies.jar` with named flags instead of positional
# arguments, and forwards anything it doesn't recognize straight through to Synthea (so
# `--exporter.*` / `--generate.*` / any other synthea.properties override still works).
#
# Examples:
#   ./generate_synthea_data.sh -p 10000 --state "New York"
#     (equivalent to the old generate_synthea_all_data.sh 10000 "New York")
#
#   ./generate_synthea_data.sh -m covid19 -p 10000 --state "New York"
#     (equivalent to the old generate_synthea_module_data.sh covid19 10000 "New York")
#
#   ./generate_synthea_data.sh -m covid19 -m sepsis -p 500 --state Utah --city "Salt Lake City"
#   ./generate_synthea_data.sh -p 1000 --state Texas --output-dir /data/ohdsi/output/texas/
#   ./generate_synthea_data.sh -p 100 --state Utah --gender F --age 60-65 --seed 21
#   ./generate_synthea_data.sh -p 10 --state Texas --dry-run
#   ./generate_synthea_data.sh -p 10 --state Texas --exporter.fhir.export=true   # passthrough

set -euo pipefail

SYNTHEA_HOME="/root/synthea"
JAR="${SYNTHEA_HOME}/synthea-with-dependencies.jar"
DEFAULT_MODULES_DIR="${SYNTHEA_HOME}/modules/"

usage() {
  cat <<'EOF'
Usage: generate_synthea_data.sh [options] [-- extra-java-args...]

Population / cohort:
  -p, --population N          Number of patients to generate
  -m, --module NAME           Restrict generation to this module (repeatable, e.g.
                               -m covid19 -m sepsis). Omit to run the full default
                               module set, same as the old "all data" script.
  --state NAME                US state to generate patients from (positional in
                               plain Synthea; a named flag here to avoid ordering
                               mistakes). Quote multi-word names: --state "New York"
  --city NAME                 City within --state. Requires --state.
  -g, --gender M|F            Restrict to one gender
  -a, --age MIN-MAX           Restrict to an age range, e.g. 60-65
  -s, --seed N                Population seed
  --clinician-seed N          Clinician seed (-cs)
  --single-person-seed N      Generate one specific person by seed (-ps)

Time window:
  -r, --reference-date YYYYMMDD   Simulation "now" (-r)
  -e, --end-date YYYYMMDD         Simulation end date (-e)
  -E, --end-date-force YYYYMMDD   Same as --end-date but bypasses Synthea's
                                   future-date-vs-years_of_history safety check (-E)

Modules / config:
  -d, --modules-dir PATH      Local modules directory to load in addition to the
                               built-in modules (default: /root/synthea/modules/,
                               only passed through if it actually exists)
  -c, --config PATH           Local .properties config file (-c)
  -o, --overflow true|false   Allow overflow population (-o)
  -k, --keep-patients PATH    Keep-matching-patients module path (-k)
  -f, --fixed-record PATH     Fixed record path (-f)
  -i, --initial-snapshot PATH Initial population snapshot (-i)
  -u, --updated-snapshot PATH Updated population snapshot output (-u)
  -t, --update-days N         Update time period in days (-t)
  --flexporter-mapping PATH   Flexporter mapping file (-fm)
  --ig PATH                   Implementation guide directory (-ig)

Export:
  --output-dir PATH           Where to write generated data (exporter.baseDirectory)
  --no-csv                    Don't set exporter.csv.export=true (on by default,
                               matching the old scripts)
  --no-uuid-filenames         Don't set exporter.use_uuid_filenames=true (on by
                               default, matching the old scripts)
  --fhir                      Also set exporter.fhir.export=true

Misc:
  --dry-run                   Print the java command instead of running it
  -h, --help                  Show this help and exit

Anything else that looks like --key=value or --key.with.dots=value is passed
straight through to Synthea as a --config override (last one wins, per Synthea's
own rules). A bare -- stops all parsing; everything after it is appended to the
java command verbatim.
EOF
}

population=""
modules=()
state=""
city=""
gender=""
age=""
seed=""
clinician_seed=""
single_person_seed=""
reference_date=""
end_date=""
end_date_force=""
modules_dir="$DEFAULT_MODULES_DIR"
config_file=""
overflow=""
keep_patients=""
fixed_record=""
initial_snapshot=""
updated_snapshot=""
update_days=""
flexporter_mapping=""
ig_dir=""
output_dir=""
csv_export=true
uuid_filenames=true
fhir_export=false
dry_run=false
passthrough=()

require_arg() {
  # $1 = flag name (for the error message), $2 = value
  if [[ -z "${2:-}" ]]; then
    echo "generate_synthea_data.sh: ${1} requires a value" >&2
    exit 2
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--population)        require_arg "$1" "${2:-}"; population="$2"; shift 2 ;;
    -m|--module)             require_arg "$1" "${2:-}"; modules+=("$2"); shift 2 ;;
    --state)                 require_arg "$1" "${2:-}"; state="$2"; shift 2 ;;
    --city)                  require_arg "$1" "${2:-}"; city="$2"; shift 2 ;;
    -g|--gender)             require_arg "$1" "${2:-}"; gender="$2"; shift 2 ;;
    -a|--age)                require_arg "$1" "${2:-}"; age="$2"; shift 2 ;;
    -s|--seed)                require_arg "$1" "${2:-}"; seed="$2"; shift 2 ;;
    --clinician-seed)        require_arg "$1" "${2:-}"; clinician_seed="$2"; shift 2 ;;
    --single-person-seed)    require_arg "$1" "${2:-}"; single_person_seed="$2"; shift 2 ;;
    -r|--reference-date)     require_arg "$1" "${2:-}"; reference_date="$2"; shift 2 ;;
    -e|--end-date)            require_arg "$1" "${2:-}"; end_date="$2"; shift 2 ;;
    -E|--end-date-force)     require_arg "$1" "${2:-}"; end_date_force="$2"; shift 2 ;;
    -d|--modules-dir)        require_arg "$1" "${2:-}"; modules_dir="$2"; shift 2 ;;
    -c|--config)              require_arg "$1" "${2:-}"; config_file="$2"; shift 2 ;;
    -o|--overflow)            require_arg "$1" "${2:-}"; overflow="$2"; shift 2 ;;
    -k|--keep-patients)      require_arg "$1" "${2:-}"; keep_patients="$2"; shift 2 ;;
    -f|--fixed-record)       require_arg "$1" "${2:-}"; fixed_record="$2"; shift 2 ;;
    -i|--initial-snapshot)   require_arg "$1" "${2:-}"; initial_snapshot="$2"; shift 2 ;;
    -u|--updated-snapshot)   require_arg "$1" "${2:-}"; updated_snapshot="$2"; shift 2 ;;
    -t|--update-days)        require_arg "$1" "${2:-}"; update_days="$2"; shift 2 ;;
    --flexporter-mapping)    require_arg "$1" "${2:-}"; flexporter_mapping="$2"; shift 2 ;;
    --ig)                     require_arg "$1" "${2:-}"; ig_dir="$2"; shift 2 ;;
    --output-dir)             require_arg "$1" "${2:-}"; output_dir="$2"; shift 2 ;;
    --no-csv)                 csv_export=false; shift ;;
    --no-uuid-filenames)     uuid_filenames=false; shift ;;
    --fhir)                   fhir_export=true; shift ;;
    --dry-run)                 dry_run=true; shift ;;
    -h|--help)                 usage; exit 0 ;;
    --)                        shift; passthrough+=("$@"); break ;;
    --*=*)                     passthrough+=("$1"); shift ;;
    *)
      echo "generate_synthea_data.sh: unrecognized argument '$1'" >&2
      echo "Run with --help for usage." >&2
      exit 2
      ;;
  esac
done

if [[ -n "$population" && ! "$population" =~ ^[0-9]+$ ]]; then
  echo "generate_synthea_data.sh: --population must be a positive integer, got '${population}'" >&2
  exit 2
fi

if [[ -n "$city" && -z "$state" ]]; then
  echo "generate_synthea_data.sh: --city requires --state" >&2
  exit 2
fi

if [[ -n "$gender" && "$gender" != "M" && "$gender" != "F" ]]; then
  echo "generate_synthea_data.sh: --gender must be M or F, got '${gender}'" >&2
  exit 2
fi

if [[ -n "$age" && ! "$age" =~ ^[0-9]+-[0-9]+$ ]]; then
  echo "generate_synthea_data.sh: --age must be MIN-MAX, e.g. 60-65, got '${age}'" >&2
  exit 2
fi

args=()

# csv export / uuid filenames on by default, matching the old scripts; --output-dir
# and --fhir are also just config overrides. These go first so an explicit passthrough
# override (e.g. --exporter.csv.export=false) still wins -- Synthea uses last-value-wins.
$csv_export && args+=("--exporter.csv.export=true")
$uuid_filenames && args+=("--exporter.use_uuid_filenames=true")
$fhir_export && args+=("--exporter.fhir.export=true")
[[ -n "$output_dir" ]] && args+=("--exporter.baseDirectory=${output_dir}")

# Only pass -d if the directory actually exists, so a missing default modules dir
# doesn't turn into a hard failure in an environment that doesn't have one.
if [[ -n "$modules_dir" && -d "$modules_dir" ]]; then
  args+=("-d" "$modules_dir")
fi

if [[ ${#modules[@]} -gt 0 ]]; then
  joined="$(IFS=:; echo "${modules[*]}")"
  args+=("-m" "$joined")
fi

[[ -n "$population" ]]         && args+=("-p" "$population")
[[ -n "$seed" ]]               && args+=("-s" "$seed")
[[ -n "$clinician_seed" ]]     && args+=("-cs" "$clinician_seed")
[[ -n "$single_person_seed" ]] && args+=("-ps" "$single_person_seed")
[[ -n "$reference_date" ]]     && args+=("-r" "$reference_date")
[[ -n "$end_date" ]]           && args+=("-e" "$end_date")
[[ -n "$end_date_force" ]]     && args+=("-E" "$end_date_force")
[[ -n "$gender" ]]             && args+=("-g" "$gender")
[[ -n "$age" ]]                && args+=("-a" "$age")
[[ -n "$config_file" ]]        && args+=("-c" "$config_file")
[[ -n "$overflow" ]]           && args+=("-o" "$overflow")
[[ -n "$keep_patients" ]]      && args+=("-k" "$keep_patients")
[[ -n "$fixed_record" ]]       && args+=("-f" "$fixed_record")
[[ -n "$initial_snapshot" ]]   && args+=("-i" "$initial_snapshot")
[[ -n "$updated_snapshot" ]]   && args+=("-u" "$updated_snapshot")
[[ -n "$update_days" ]]        && args+=("-t" "$update_days")
[[ -n "$flexporter_mapping" ]] && args+=("-fm" "$flexporter_mapping")
[[ -n "$ig_dir" ]]             && args+=("-ig" "$ig_dir")

args+=("${passthrough[@]}")

# state/city are positional in Synthea itself, and must come last.
[[ -n "$state" ]] && args+=("$state")
[[ -n "$city" ]]  && args+=("$city")

if $dry_run; then
  printf 'java -jar %q' "$JAR"
  for a in "${args[@]}"; do printf ' %q' "$a"; done
  printf '\n'
  exit 0
fi

WD=$(pwd)
cd "$SYNTHEA_HOME"
java -jar "$JAR" "${args[@]}"
cd "$WD"
