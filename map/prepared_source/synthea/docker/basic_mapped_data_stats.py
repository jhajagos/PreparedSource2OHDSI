import preparedsource2ohdsi.mapping_utilities as mu
import pyspark
import json
spark = pyspark.sql.SparkSession.builder\
    .config("spark.driver.memory", "16g") \
    .getOrCreate()

with open("/root/config/prepared_source_to_ohdsi_config.json.generated.parquet.json") as f:
    tbs = json.load(f)

catalog = mu.attach_catalog_dict(tbs)

statistics_queries = {"count_person": "select count(distinct person_id) as n, count(*) as n_r from person",
                      "count_visits": "select count(distinct person_id) as n, count(*) as n_r from vist_occurrence"}

for tag in statistics_queries:

    print(f"{tag}:")
    query = statistics_queries[tag]
    print(query)
    print(spark.sql(query).toPandas())
    print("")
