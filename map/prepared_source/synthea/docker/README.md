# Synthea to OHDSI RDBMS

This Docker container provides a containerized pipeline for mapping Synthea data into the OHDSI CDM. The mapping is
done within a Spark environment. OHDSI tool chains are optimized for relational databases (RDBMS).

## Building the Docker Image


```bash
docker build -t syntheaohdsi:latest ./ 
```

Once you follow the process of building the vocabularies:

The directory: `/home/user/data/vocabulary/20231114` should contain `bz2` compressed files for each 
vocabulary files.

```bash
cd /home/user/data/vocabulary/20231114
bzip2 -v *.csv
```

```bash ""
docker run --rm -it -v /home/user/data/vocabulary/20231114:/data/ohdsi/vocabulary \
  -v /home/user/data/synthea/covid19:/data/ohdsi/output \
  -v /home/user/jdbc:/root/jdbc \
  --name syntheohdsi --hostname syntheaohdsi \
  syntheaohdsi:latest /bin/bash 
```

The following steps assume you are running within the container.


## Staging the concept files for mapping

You will only need to run this once:

```bash
conda activate PySpark
cd /root/scripts
./build_vocabulary_files_for_mapping.sh
```

## Generating synthetic OHDSI data

You can run multiple files 

## Loading into a relational database


```bash
docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=zzZZZZZ" -p 1433:1433 --name sql1 --hostname sql1 -d mcr.microsoft.com/mssql/server:2022-latest
```
```bash
docker network create ohdsi
docker network connect ohdsi sql1
docker netwrok connect ohdsi syntheohdsi
```


```
python ./db_configure.py -j 'jdbc:sqlserver://sql1:1433;encrypt=false;database=synthea_ohdsi' -u sa -p zzZZZZZ
```

