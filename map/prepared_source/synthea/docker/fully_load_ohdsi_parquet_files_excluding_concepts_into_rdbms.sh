#!/usr/bin/env bash

WD=$(pwd)

source ./stage_tables_excluding_concepts_into_rdbms.sh || exit
source load_all_staged_tables_into_ohdsi_rdbms.sh || exit

cd $WD