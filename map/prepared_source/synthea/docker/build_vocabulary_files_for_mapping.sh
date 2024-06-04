#!/usr/bin/env bash

WD=$(pwd)
cd /root/github/PreparedSource2OHDSI/map/ohdsi/
python build_concept_tables_for_mapping.py -l -c /root/config/prepared_source_to_ohdsi_config.json

cd $WD
