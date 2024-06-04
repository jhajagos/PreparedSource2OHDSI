# Synthea

Synthea is a program for generating synthetic FHIR data sets.  See 
https://synthea.mitre.org/downloads for datasets to download.

## CSV files

CSV files will be staged in an accessible storage location on a SPARK cluster. For more 
information on Synthea:
https://github.com/synthetichealth/synthea/wiki/CSV-File-Data-Dictionary

## Running the pipeline

### Prepared Source 

```json
{
  "base_path": "/mnt/data_hf/synthea/covid19/100K/",
  "file_type": "csv",
  "csv_extension": ".csv",
  "prepared_source_output_location": "/mnt/data_hf/synthea/covid19/100K/output/prepared_source/"
}
```

### Building OHDSI Concept Tables

### Generating OHDSI Tables

Create configuration file:

```json
{
  "concept_base_path": "/mnt/data_hf/ohdsi/vocabulary/20220830/",
  "concept_csv_file_extension": ".csv.bz2",
  "export_concept_mapping_table_path": "/mnt/data_hf/ohdsi/vocabulary/20220830/mapping/",
  "prepared_source_table_path": "/mnt/data_hf/synthea/covid19/100K/output/prepared_source/",
  "ohdsi_output_location": "/mnt/data_hf/synthea/covid19/100K/ohdsi/",
  "check_pointing": "NONE"
}
```

Generate Parquet files in PSF (Prepared Source Format)

```bash
cd ~/github/ScaleClinicalDataPipelines/
cd ./
python ./map_synthea_to_prepared_source.py -c ./config.100K.json
```
### Creating an OHDSI database

Generate Parquet files in OHDSI 5.3.1 format:

```bash
python ./map_prepared_source_to_ohdsi_cdm.py -c ./config/config_synthea_covid.100k.json
```
