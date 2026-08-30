#!/usr/bin/env bash

WD=$(pwd)
cd /root/synthea
# exporter.fhir.export must be forced off: Synthea's bundled synthea.properties defaults
# it to true, so without this every run silently wrote full FHIR JSON bundles alongside
# the CSVs -- large enough per patient to fill the container's disk over repeated runs.
java -jar  synthea-with-dependencies.jar -d /root/synthea/modules/ --exporter.csv.export=true --exporter.use_uuid_filenames=true --exporter.fhir.export=false -m covid19 -p $1 "New York"

# Only the CSV output is consumed downstream (map_synthea_to_prepared_source.py); belt
# and suspenders in case a future edit re-enables FHIR export above.
fhir_dir="/root/synthea/output/fhir"
if [[ -d "$fhir_dir" ]]; then
  json_count=$(find "$fhir_dir" -name '*.json' | wc -l)
  if [[ "$json_count" -gt 0 ]]; then
    echo "generate_synthetic_covid19.sh: removing ${json_count} leftover FHIR JSON file(s) from ${fhir_dir}" >&2
    find "$fhir_dir" -name '*.json' -delete
  fi
fi

cd $WD