### Scaling mapping EHR (Electronic Health Record) data to the OHDSI CDM 

The goal of this project is to scale the mapping of clinical data to the [OHDSI 
CDM](https://ohdsi.github.io/CommonDataModel/cdm54.html) (Common Data Model).
The scalable compute is provided by using a SPARK (>3.0) compute environment.
Data is written in the [Apache Parquet format](https://parquet.apache.org/)
and can be directly queried or staged into a relational SQL database
or in delta tables.

The mapping from source to OHDSI consists of the following steps:

1. Map EHR data to PSF (Prepared Source Format) CSV format (See Synthea example) 
1. Stage CSV files as parquet files in a SPARK Cluster accessible location
1. Stage OHDSI Vocabulary/Concept (TSV) files as parquet file 
1. Map PSF to the OHDSI CDM (Currently supported are 5.3.1 and 5.4 versions) Parquet Format
1. Register generated parquet files in a database catalog (Delta tables) or insert data into an OHDSI CDM database

The environment writes parquet files in an OHDSI "compatible" format. The generated
parquet files include additional fields not part of the OHDSI CDM.
