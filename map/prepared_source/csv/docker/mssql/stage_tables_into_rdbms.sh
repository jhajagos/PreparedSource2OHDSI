#!/usr/bin/env bash

WD=$(pwd)
cd /root/github/PreparedSource2OHDSI/map/ohdsi/utilities/sqlserver
python transfer_parquet_files_to_sql_server.py -j /root/config/prepared_source_to_ohdsi_config.json.generated.parquet.json \
 -c /root/config/prepared_source_to_ohdsi_config.json -s dbo -l

cd $WD