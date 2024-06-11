# DataBricks 

Publish the OHDSI database:

```bash
python ./publish_ohdsi_database.py ./publish_ohdsi_database.py -n synthea_covid_100K \ 
-j ./config/config_synthea_covid.100k.json.generated.parquet.json -d /mnt/data_hf/synthea/covid19/100K/ohdsi/db/
```

Populate CDM Source Table

Generate the JSON `cdm_source.json`
```json
{
  "cdm_source_name": null,
  "cdm_source_abbreviation": null,
  "cdm_holder": "SBM",
  "source_description": "Synthea",
  "source_documentation_reference": "https://synthea.mitre.org/downloads",
  "cdm_etl_reference": "https://github.bmi.stonybrookmedicine.edu/jhajagos/ScaleClinicalDataPipelines",
  "source_release_date": null,
  "cdm_release_date": null,
  "cdm_version": "CDM v5.3.1"
}
```

```bash
python populate_cdm_source.py -n synthea_covid_100K -m cdm_source.json

```

### Running Achilles and DQD

Create the results database:
```sql
create database  synthea_covid_100K_results
```


```R
library("Achilles")
Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = "/tmp/")
downloadJdbcDrivers("spark")
cdmDatabaseSchema <- "synthea_covid_100K" # the fully qualified database schema name of the CDM
resultsDatabaseSchema <- "synthea_covid_100K_results"
sourceName <- "Synthea Covid 100K"
sparkConnectionString <- "jdbc:spark://adb-2663131608781029.9.azuredatabricks.net:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/2663131608781029/0429-160712-n9csffwo;AuthMech=3;UID=TOKEN;PWD=TOKEN;ignoreTransactions=1"
```

```R
achilles(connection, cdmDatabaseSchema = cdmDatabaseSchema, resultsDatabaseSchema = resultsDatabaseSchema, vocabDatabaseSchema = cdmDatabaseSchema, sourceName = sourceName, cdmVersion = 5.3, optimizeAtlasCache = TRUE)
```

Setting up the DQD environment
```R
numThreads <- 1 # on Redshift, 3 seems to work well

# specify if you want to execute the queries or inspect them ------------------------------------------
sqlOnly <- FALSE # set to TRUE if you just want to get the SQL scripts and not actually run the queries

# where should the logs go? -------------------------------------------------------------------------
outputFolder <- "output"
outputFile <- paste(resultsDatabaseSchema, ".json")

# logging type -------------------------------------------------------------------------------------
verboseMode <- FALSE # set to TRUE if you want to see activity written to the console

# write results to table? ------------------------------------------------------------------------------
writeToTable <- TRUE

checkLevels <- c("TABLE", "FIELD", "CONCEPT")

# which DQ checks to run? ------------------------------------

checkNames <- c("measurePersonCompleteness", "isPrimaryKey", "isForeignKey", "fkDomain", "fkClass", "isStandardValidConcept",
 "measureValueCompleteness",  "standardConceptRecordCompleteness", "sourceConceptRecordCompleteness",
 "plausibleTemporalAfter", "plausibleDuringLife", "plausibleGender", "plausibleValueLow", "plausibleValueHigh")

tablesToExclude <- c() 
```

Executing the DQD (Data quality dashboard)
```R
DataQualityDashboard::executeDqChecks(connectionDetails = connection, 
                                    cdmDatabaseSchema = cdmDatabaseSchema, 
                                    resultsDatabaseSchema = resultsDatabaseSchema,
                                    cdmSourceName = sourceName, 
                                    numThreads = numThreads,
                                    sqlOnly = sqlOnly, 
                                    outputFolder = outputFolder, 
                                    outputFile = outputFile,
                                    verboseMode = verboseMode,
                                    writeToTable = writeToTable,
                                    checkLevels = checkLevels,
                                    tablesToExclude = tablesToExclude,
                                    cdmVersion = "5.3",
                                    checkNames = checkNames)
```

Viewing the dashboard

```R
DataQualityDashboard::viewDqDashboard("/home/rstudio/output/synthea_covid_100K_results.json")
```

### Connecting to Atlas

```sparksql
create database ohdsi_temp2
```

Adding source information to Atlas
```postgresql
INSERT INTO ohdsi.source (source_id, source_name, source_key, source_connection, source_dialect, cached) 
    VALUES (3, 'synthea_covid_100K', 'synthea_covid_100K', 'jdbc:spark://adb-2663131608781029.9.azuredatabricks.net:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/2663131608781029/0429-160712-n9csffwo;AuthMech=3;UID=token;PWD=', 'spark', false);

INSERT INTO ohdsi.source_daimon (source_id, daimon_type, table_qualifier, priority) 
    values (3, 0,'synthea_covid_100K', 0);

INSERT INTO ohdsi.source_daimon (source_id, daimon_type, table_qualifier, priority) 
    VALUES (3, 1, 'synthea_covid_100K', 1);

INSERT INTO ohdsi.source_daimon (source_id, daimon_type, table_qualifier, priority) 
    VALUES (3, 5, 'ohdsi_temp2', 0);

INSERT INTO ohdsi.source_daimon (source_id, daimon_type, table_qualifier, priority) 
    VALUES (3, 2, 'synthea_covid_100K_results', 1);
```

Customize ths as needed
```bash
curl http://localhost:8080/WebAPI/ddl/results?dialect=spark&schema=synthea_covid_100K_results&vocabSchema=synthea_covid_100k&tempSchema=ohdsi_temp2&initConceptHierarchy=true
```

You will need to capture the output and execute it against the database.