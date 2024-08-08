### Scaling EHR (Electronic Health Record) data mapping to the OHDSI CDM 

The goal of this project is to scale the mapping of clinical data to the [OHDSI 
CDM](https://ohdsi.github.io/CommonDataModel/cdm54.html) (Common Data Model).
The scalable compute is provided by using a SPARK (>3.0) compute environment.
Data is written in the [Apache Parquet format](https://parquet.apache.org/)
and can be either directly queried or staged into a relational SQL database.

The mapping from source to OHDSI consists of the following steps:

1. Stage CSV files extracted from the EHR in a SPARK Cluster accessible location
1. Map stage CSV data to PSF (Prepared Source Format) format (See Synthea example) 
1. Stage OHDSI Vocabulary/Concept (TSV) files as parquet file 
1. Map PSF to the OHDSI CDM (Currently supported are 5.4 and 5.3.1 and versions) Parquet Format
1. Register generated parquet files in a database catalog (Delta tables) or insert parquet files into
a relational database.

The mapping scripts writes parquet files in an OHDSI "compatible" format. The generated
parquet files include additional fields not part of the OHDSI CDM. The additional columns
allow the tracking of the initial data provence.

A Docker biuld file is included to map Synthea data to PSF and to OHDSI; see: 
[README.md](map%2Fprepared_source%2Fsynthea%2Fdocker%2FREADME.md).
