#!/usr/bin/env bash

WD=$(pwd)

source ./stage_tables_into_rdbms.sh || exit
source ./load_staged_tables_into_ohdsi_rdbms_cdm.sh || exit

cd $WD