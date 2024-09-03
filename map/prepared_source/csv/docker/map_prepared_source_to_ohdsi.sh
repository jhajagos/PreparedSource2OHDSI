#!/usr/bin/env bash

WD=$(pwd)
cd /root/github/PreparedSource2OHDSI/map/ohdsi/
python  map_prepared_source_to_ohdsi_cdm.py  -l -c /root/config/prepared_source_to_ohdsi_config.json

cd $WD