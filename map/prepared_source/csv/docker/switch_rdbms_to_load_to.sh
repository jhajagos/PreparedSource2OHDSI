#!/usr/bin/env bash

#  To switch the RDBMS to Microsoft SQL Server
# ./switch_rdbms_to_load_to.sh mssql
#
# To switch back to Postgres SQL
# ./switch_rdbms_to_load_to.sh psql

cd /root/scripts/
rm load_all_staged_tables_into_ohdsi_rdbms.sh load_staged_tables_into_ohdsi_rdbms.sh stage_tables_excluding_concepts_into_rdbms.sh stage_tables_into_rdbms.sh

ln -s ./$1/*.sh ./