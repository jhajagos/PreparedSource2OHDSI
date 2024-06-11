# Synthea to OHDSI RDBMS (Relational Database Management System)

This Docker container provides a pipeline for rapidly mapping synthetic data into the [OHDSI CDM](https://ohdsi.github.io/CommonDataModel/cdm54.html) (Common Data Model). 
[Synthea](https://synthetichealth.github.io/synthea/)
generates synthetic data which does not have the same restrictions as EHR data in terms of privacy and  
human research restrictions.

The mapping is done in a local [Spark](https://spark.apache.org/) environment which generates Parquet files that are 
aligned to the OHDSI CDM. As the OHDSI tool chains are optimized for relational databases (RDBMS) we then load the Parquet files into the RDBMS. 
For this container, we focus on loading into Microsoft SQL Server but the approach here could be easily adapted to other RDBMS, 
such as, PostGreSQL. Microsoft SQL Server is widely used to host OHDSI CDM databases at different research institutions and is available 
as an easy to run Docker container.

For an introduction to Docker see: https://docker-curriculum.com/ and 
navigating the bash shell in a Linux environment https://ubuntu.com/tutorials/command-line-for-beginners#1-overview.

## Building the Synthea OHDSI Docker Image

For development purposes, I have Docker installed on my local machine. I have tested the mapping of 10,000 patients on
a machine with 64Gb of RAM. 

The first step is to check out the repository from Github:
```bash
cd ~
mkdir github
cd ./github
git clone https://github.com/jhajagos/PreparedSource2OHDSI.git
```

Then it is to build/compile the repository code:
```bash
cd ~
cd ./github/PrepardSource2OHDSI
cd ./map/repared_source/synthea/docker/
docker build -t syntheaohdsi:latest ./ 
```

## Preparing concept files
The one external dependency on mapping data to the OHDSI CDM is generating concept files. You can follow the process of building the concept/vocabularies files; see https://github.com/OHDSI/Vocabulary-v5.0/wiki
and https://athena.ohdsi.org/vocabulary/list. My general vocabulary list for working with EHR (Electronic Health Record) data is:

```
NUCC	-	National Uniform Claim Committee Health Care Provider Taxonomy Code Set (NUCC)
Revenue Code	-	UB04/CMS1450 Revenue Codes (CMS)
CVX	-	CDC Vaccine Administered CVX (NCIRD)
NAACCR	-	Data Standards & Data Dictionary Volume II (NAACCR)
LOINC	-	Logical Observation Identifiers Names and Codes (Regenstrief Institute)
HemOnc	-	HemOnc
ICDO3	-	International Classification of Diseases for Oncology, Third Edition (WHO)
SNOMED	-	Systematic Nomenclature of Medicine - Clinical Terms (IHTSDO)
Gender	-	OMOP Gender
NDC	-	National Drug Code (FDA and manufacturers)
OMOP Invest Drug	-	OMOP Investigational Drugs
RxNorm Extension	-	OMOP RxNorm Extension
SPL	-	Structured Product Labeling (FDA)
ICD9Proc	-	International Classification of Diseases, Ninth Revision, Clinical Modification, Volume 3 (NCHS)
CPT4	-	Current Procedural Terminology version 4 (AMA)
CMS Place of Service	-	Place of Service Codes for Professional Claims (CMS)
Currency	-	International Currency Symbol (ISO 4217)
Race	-	Race and Ethnicity Code Set (USBC)
ICD9CM	-	International Classification of Diseases, Ninth Revision, Clinical Modification, Volume 1 and 2 (NCHS)
ABMS	-	Provider Specialty (American Board of Medical Specialties)
ICD10PCS	-	ICD-10 Procedure Coding System (CMS)
Multum	-	Cerner Multum (Cerner)
ICD10CM	-	International Classification of Diseases, Tenth Revision, Clinical Modification (NCHS)
CGI	-	Cancer Genome Interpreter (Pompeu Fabra University)
Cancer Modifier	-	Diagnostic Modifiers of Cancer (OMOP)
RxNorm	-	RxNorm (NLM)
ATC	-	WHO Anatomic Therapeutic Chemical Classification
Ethnicity	-	OMOP Ethnicity
HCPCS	-	Healthcare Common Procedure Coding System (CMS)
VA Class	-	VA National Drug File Class (VA)
Medicare Specialty	-	Medicare provider/supplier specialty codes (CMS)
OMOP Extension	-	OMOP Extension (OHDSI)
```
As the Synthea data does not contain CPT codes you do not need to have a UMLS account.

We compress the concept files to make them portable and faster to read in Spark:
```bash
cd /home/user/data/vocabulary/20231114
bzip2 -v *.csv
```

The directory: `/home/user/data/vocabulary/20231114` should now contain `bz2` compressed files for each 
vocabulary file and will be mounted to the running container.

## Running the container

Once the container is compiled/built you will need to run it.
```bash
docker run -it \
  --name syntheohdsi --hostname syntheaohdsi \
  -v /home/user/data/vocabulary/20231114:/data/ohdsi/vocabulary  \
  -v /home/user/data/synthea/covid19:/data/ohdsi/output \  
  -v /home/user/jdbc:/root/jdbc \ 
  -v /home/user/synthea/modules:/root/synthea/modules \
  syntheaohdsi:latest /bin/bash 
```

For testing purposes, I usually add the `--rm` option to remove the container after exiting which helps save space. 
The remaining steps assume you are running within the container and have activated the conda `PySpark` environment
```bash
conda activate PySpark
```

## Staging the concept files for mapping

You will only need to run this once as it will write the Parquet files to the mounted volume. 
It generates Parquet file versions of the OHDSI concept tables that are used for mapping. The OHDSI Parquet files
contain some additional columns that are not in the OHDSI CDM to help link tables back to the source data.

```bash
cd /root/scripts
./build_vocabulary_files_for_mapping.sh
```

## Generating synthetic OHDSI data

I have created a simple script for generating Synthetic Covid patients from New York state.
```bash
cd /root/scripts 
./generate_synthetic_covid 10000 # Generate 10000 patients
```
This above script could be easily copied and modified to generate your own specific synthetic patient data. Synthea has a large number
of prebuilt modules and the ability to create your own using a JSON file: see the [module builder](https://github.com/synthetichealth/module-builder)
and the [generic framework](https://github.com/synthetichealth/synthea/wiki/Generic-Module-Framework). You can test synthetic 
patient data generation outside this container.

To understand how the synthetic data is generated with Synthea; here is a sample commandline execution of the tool:
```
java -jar  synthea-with-dependencies.jar -d /root/synthea/modules/ --exporter.csv.export=true -m covid19 -p 10000 "New York"
```
This command tells Synthea to generate CSV output by only running the module `covid19` and generate 10000 patients from New York 
State. More information on running the Synthea commandline tool can be found at: https://github.com/synthetichealth/synthea/wiki/Basic-Setup-and-Running. 

In the container the CSV files are created in the directory `/root/synthea/output/csv`. An example of files generated:
```
(PySpark) root@46a786d86509:~/synthea/output/csv# ls -sh *.csv
4.0K allergies.csv   33M claims.csv               704K conditions.csv   26M encounters.csv        18M immunizations.csv   37M observations.csv   2.6M patients.csv           4.0K payers.csv      300K providers.csv
308K careplans.csv  348M claims_transactions.csv   12K devices.csv     4.0K imaging_studies.csv  812K medications.csv    252K organizations.csv   18M payer_transitions.csv  724K procedures.csv  1.2M supplies.csv
```

The next step is to map the CSV files generated by the Synthea tool into OHDSI files.
```bash
./map_synthea_csv_to_spark_ohdsi.sh
```
This is a two-step process: 1) Map Synthea generated CSV files into the PSF (Prepared Source Format) 2) Map PSF
data into the OHDSI CDM. If you are interested in how the Synthea CSV is converted to PSF see: [map_synthea_to_prepared_source.py](..%2Fmap_synthea_to_prepared_source.py). 
If you can write your own local EHR (Electronic Health Record) data into PSF then you can use the 
mapper script [map_prepared_source_to_ohdsi_cdm.py](..%2F..%2F..%2Fohdsi%2Fmap_prepared_source_to_ohdsi_cdm.py). There are 
SQL templates that are easily modified; see: [prepared_source_sql_template.sql](..%2F..%2F..%2F..%2Fsrc%2Fprepared_source_sql_template.sql).

If you want to directly query the generated Parquet files from within the container look at 
[basic_mapped_data_stats.py](basic_mapped_data_stats.py) for how to attach the parquet files into your local Spark environment. 
For basic analytical querying you can use the ipython shell for querying. 

## Loading OHDSI into a relational database management system (RDBMS)

To emulate an environment found at a research institution we will load the data into a relational database. Here we will
use the SQL Server Docker Image and run it locally.

### Setting up SQL Server

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

Configure the JDBC driver and the connection string:
```bash
python ./db_configure.py -j 'jdbc:sqlserver://sql1:1433;encrypt=false;database=synthea_ohdsi' -u sa -p zzZZZZZ
```
The JDBC driver can be [downloaded](https://learn.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server?view=sql-server-ver16.) from
Microsoft.
 
You may need to modify the script `load_staged_tables_into_ohdsi_rdbms_cdm.sh` if you use a different version of the JDBC driver 
than: `mssql-jdbc-12.6.2.jre11.jar` 
by using the `-j` option for  [jdbc_sql_loader.py](jdbc_sql_loader.py) and pointing to the updated JAR file.

### First time loading

You will need to have created an empty database on the SQL Server system and loaded the DDL 
[OHDSI 5.4 schema for SQL sever](https://github.com/OHDSI/CommonDataModel/blob/main/ddl/5.4/sql_server/OMOPCDM_sql_server_5.4_ddl.sql).

The first scripts transfers the Parquet files to the RDBMS using Spark's optimized code. 
```bash
cd /root/scripts
./map_synthea_csv_to_ohdsi_parquet.sh
```
This script creates tables, such as, `dbo.transferConcept`.

This next scripts loads them into the OHDSI CDM defined tables using a SQL script. 
```bash
cd /root/scripts
./fully_load_ohdsi_parquet_files_into_rdbms.sh
```
Also, the script [transfer_sql_inserts_54.sql](..%2F..%2F..%2Fohdsi%2Futilities%2Fsqlserver%2Ftransfer_sql_inserts_54.sql)
could also be run in your favorite SQL editor.

### Refreshing the relational database (RDBMS)

Once you have run through the setup of the containers refreshing the generated data from Synthea to the RDBMS requires only 
running three scripts:
```bash
cd /root/scripts 
./generate_synthetic_covid 10000 # Generate 10000 patients
./map_synthea_csv_to_ohdsi_parquet.sh # Generate parquet files that align with the OHDSI CDM
./fully_load_ohdsi_parquet_files_into_rdbms.sh # Load parquet files to a RDBMS system (Microsoft SQL Server)
```

Now you can rapidly generate synthetic OHDSI data for testing data manipulation pipelines and cohort selection algorithms.