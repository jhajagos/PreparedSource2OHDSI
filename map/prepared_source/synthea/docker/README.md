# Synthea to OHDSI RDBMS

This Docker container provides a containerized pipeline for mapping Synthea data into the OHDSI CDM. The mapping is
done within a Spark environment and generates Parquet files that are aligned to the OHDSI CDM. 
As the OHDSI tool chains are optimized for relational databases (RDBMS) we have scripts for mapping the data. For this container
we focus on loading into Microsoft SQL Server but the approach here could be easily adapted to other databases. 
Microsoft SQL Server is widely used to host OHDSI databases at different health systems and is available as a 
Docker container.

## Building the Synthea OHDSI Docker Image

For development purposes I have Docker installed on my local machine. 

Check out the repository code:
```bash
cd /home/user/
mkdir github
cd ./github
git clone https://github.com/jhajagos/PreparedSource2OHDSI.git
```

Build the repository code
```bash
docker build -t syntheaohdsi:latest ./ 
```

## Preparing concept files
Once you follow the process of building the concept/vocabularies files you will need to compress them.

```bash
cd /home/user/data/vocabulary/20231114
bzip2 -v *.csv

```

The directory: `/home/user/data/vocabulary/20231114` should contain `bz2` compressed files for each 
vocabulary file.


## Running the container
```bash ""
docker run -it \
  --name syntheohdsi --hostname syntheaohdsi \
  -v /home/user/data/vocabulary/20231114:/data/ohdsi/vocabulary  \
  -v /home/user/data/synthea/covid19:/data/ohdsi/output \  
  -v /home/user/jdbc:/root/jdbc \ 
  -v /home/user/synthea/modules:/root/synthea/modules \
  syntheaohdsi:latest /bin/bash 
```

For testing purposes I usually add the `--rm` option to remove the container after exiting.

The following steps assume you are running within the container.


## Staging the concept files for mapping

You will only need to run this once. It generates Parquet file versions of the OHDSI tables

```bash
conda activate PySpark
cd /root/scripts
./build_vocabulary_files_for_mapping.sh
```

## Generating synthetic OHDSI data

I have created a simple script for generating Synthetic Covid patients from New York state.
```bash
cd /root/scripts 
./generate_synthetic_covid 10000 # Generate 10000 patients
```
This above script could be replaced to generate your specific synthetic patient data. Synthea has a large number
of prebuilt modules plus the abilty to create your own: see

Parquet files:
```bash
./map_synthea_csv_to_spark_ohdsi.sh
```
If you want to directly query these files from within the container look at `basic_mapped_data_stats.py` for how to attach the
parquet files into your local Spark environment. For basic analytical querying this should be the final step.

## Loading OHDSI into a relational database management system (RDBMS)

To emulate an environment found at a research institution we will load the data into a relational database. Here we will
use the SQL Server Docker Image to run locally.

### Setting up the environment

Run the container
```bash
docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=zzZZZZZ" -p 1433:1433 --name sql1 --hostname sql1 -d mcr.microsoft.com/mssql/server:2022-latest
```

Create a network and connect the two containers:
```bash
docker network create ohdsi
docker network connect ohdsi sql1
docker network connect ohdsi syntheaohdsi
```
[basic_mapped_data_stats.py](basic_mapped_data_stats.py)
Configure the JDBC driver and the connection string:
```bash
python ./db_configure.py -j 'jdbc:sqlserver://sql1:1433;encrypt=false;database=synthea_ohdsi' -u sa -p zzZZZZZ
```
The JDBC driver for Microsoft can be [downloaded](https://learn.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server?view=sql-server-ver16.) from Microsoft.
 
You may need to modify the script `load_staged_tables_into_ohdsi_rdbms_cdm.sh` if you use a different version of the JDBC driver than: `mssql-jdbc-12.6.2.jre11.jar` 
by using the `-j` option and pointing to the updated JDBC file.

### First run through

You will need to have created a database on the SQL Server system and loaded the DDL 
[OHDSI 5.4 schema for SQL sever](https://github.com/OHDSI/CommonDataModel/blob/main/ddl/5.4/sql_server/OMOPCDM_sql_server_5.4_ddl.sql).

The first scripts transfers the Parquet files to the RDBMS using Spark's optimized code. 
```bash
cd /root/scripts
./map_synthea_csv_to_ohdsi_parquet.sh
```
This script creates tables such as `dbo.transferConcept`

This next scripts loads them into the OHDSI CDM defined tables using a SQL script. 
```bash
cd /root/scripts
./fully_load_ohdsi_parquet_files_into_rdbms.sh
```
The script [transfer_sql_inserts_54.sql](..%2F..%2F..%2Fohdsi%2Futilities%2Fsqlserver%2Ftransfer_sql_inserts_54.sql)
could also be run in your favorite SQL editor.

### Refreshing the relational database (RDBMS)

Once you have run through the setup of the containers refreshing the data from Synthea to the RDMBS requires only 
running three scripts:
```bash
cd /root/scripts 
./generate_synthetic_covid 10000 # Generate 10000 patients
./map_synthea_csv_to_ohdsi_parquet.sh # Generate parquet files that align with the OHDSI CDM
./fully_load_ohdsi_parquet_files_into_rdbms.sh # Load parquet files to a RDBMS system (Microsoft SQL Server)
```