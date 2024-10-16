#!/usr/bin/env bash

WD=$(pwd)
cd /root/scripts/
python jdbc_sql_loader.py -f /root/github/PreparedSource2OHDSI/map/ohdsi/utilities/sqlserver/transfer_sql_inserts_54.sql
cd $WD