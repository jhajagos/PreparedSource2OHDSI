#!/usr/bin/env bash

# This script should be utilize after you generated synthetic data. It will map
# the CSV file. It assumes that you have processed the OHDSI vocabulary data
# have mounted them as volume

source ./map_synthea_csv_to_prepared_source.sh || exit

source ./map_prepared_source_to_ohdsi.sh || exit

conda activate PySpark

python basic_mapped_data_stats.py -l || exit