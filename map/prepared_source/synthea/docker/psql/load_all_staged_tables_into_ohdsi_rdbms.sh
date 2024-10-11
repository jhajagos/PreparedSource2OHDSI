#!/usr/bin/env bash

WD=$(pwd)
cd /root/scripts/
python jdbc_sql_loader.py -f /root/github/PreparedSource2OHDSI/map/ohdsi/utilities/sqlserver/transfer_psql_inserts_54.sql -c org.postgresql.Driver -p /root/jdbc/postgresql-42.7.4.jar
cd $WD