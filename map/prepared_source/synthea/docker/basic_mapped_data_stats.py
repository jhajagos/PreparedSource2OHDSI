import preparedsource2ohdsi.mapping_utilities as mu
import pyspark
import json
spark = pyspark.sql.SparkSession.builder\
    .config("spark.driver.memory", "16g") \
    .getOrCreate()

with open("/root/config/prepared_source_to_ohdsi_config.json.generated.parquet.json") as f:
    tbs = json.load(f)

catalog = mu.attach_catalog_dict(spark, tbs)

statistics_queries = {"count_people": "select count(distinct person_id) as n, count(*) as n_r from person",
                      "count_visits": "select count(distinct person_id) as n, count(*) as n_r from visit_occurrence",

                      "count_deaths": "select count(distinct person_id) as n, count(*) as n_r from death",

                      "count_conditions": "select count(distinct person_id) as n, count(*) as n_r from condition_occurrence",
                      "count_procedures": "select count(distinct person_id) as n, count(*) as n_r from procedure_occurrence",

                      "count_measurements": "select count(distinct person_id) as n, count(*) as n_r from measurement",
                      "count_observations": "select count(distinct person_id) as n, count(*) as n_r from observation",

                      "count_drugs": "select count(distinct person_id) as n, count(*) as n_r from drug_exposure",
                      "count_devices": "select count(distinct person_id) as n, count(*) as n_r from device_exposure"


                      }

for tag in statistics_queries:

    print(f"{tag}:")
    query = statistics_queries[tag]
    print(query)
    print(spark.sql(query).toPandas())
    print("")
