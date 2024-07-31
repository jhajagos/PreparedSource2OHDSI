#!/usr/bin/env bash

WD=$(pwd)
cd /root/synthea
java -jar  synthea-with-dependencies.jar -d /root/synthea/modules/ --exporter.csv.export=true --exporter.use_uuid_filenames=true -m $1 -p $2 "$3"

cd $WD