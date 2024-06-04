#!/usr/bin/env bash

WD=$(pwd)
cd /root/github/PreparedSource2OHDSI/map/prepared_source/synthea/
python map_synthea_to_prepared_source.py -c ~/config/synthea_config.json

cd $WD