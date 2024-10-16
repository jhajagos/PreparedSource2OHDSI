from pyspark.sql import SparkSession
import preparedsource2ohdsi.mapping_utilities as mu
import pyspark
import json
import argparse


def main(spark, tbs):

    catalog = mu.attach_catalog_dict(spark, tbs)

    statistics_queries = {"count_people": "select count(distinct person_id) as n, count(*) as n_r from person",
                          "count_visits": "select count(distinct person_id) as n, count(*) as n_r from visit_occurrence",
                          "count_deaths": "select count(distinct person_id) as n, count(*) as n_r from death",
                          "count_observation_periods": "select count(distinct person_id) as n, count(*) as n_r from observation_period",

                          "count_gender": "select count(distinct person_id) as n, count(*) as n_r, concept_name as gender_concept_name from person p join concept c on p.gender_concept_id = c.concept_id group by gender_concept_id, concept_name order by count(*) desc",
                          "count_race": "select count(distinct person_id) as n, count(*) as n_r, concept_name as race_concept_name from person p join concept c on  p.race_concept_id = c.concept_id group by race_concept_id, concept_name order by count(*) desc",
                          "count_ethnicity": "select count(distinct person_id) as n, count(*) as n_r, concept_name as ethnicity_concept_name from person p join concept c on  p.ethnicity_concept_id = c.concept_id group by ethnicity_concept_id, concept_name order by count(*) desc",

                          "count_conditions": "select count(distinct person_id) as n, count(*) as n_r from condition_occurrence",
                          "count_procedures": "select count(distinct person_id) as n, count(*) as n_r from procedure_occurrence",

                          "count_measurements": "select count(distinct person_id) as n, count(*) as n_r from measurement",
                          "count_observations": "select count(distinct person_id) as n, count(*) as n_r from observation",

                          "count_drugs": "select count(distinct person_id) as n, count(*) as n_r from drug_exposure",
                          "count_devices": "select count(distinct person_id) as n, count(*) as n_r from device_exposure",

                          "count_payers": "select count(distinct person_id) as n, count(*) as n_r from payer_plan_period",

                          "count_visit_concepts_count": "select count(distinct person_id) as n, count(*) as n_r, visit_concept_id, c.concept_name as visit_concept_name from visit_occurrence vo join concept c on vo.visit_concept_id = c.concept_id group by visit_concept_id, c.concept_name order by count(*) desc",

                          "condition_concepts_count": "select count(distinct person_id) as n, count(*) as n_r, condition_concept_id, c.concept_name as condition_concept_name from condition_occurrence co join concept c on c.concept_id = co.condition_concept_id group by condition_concept_id, c.concept_name order by count(*) desc",

                          "procedure_concepts_count": "select count(distinct person_id) as n, count(*) as n_r, procedure_concept_id, c.concept_name as procedure_concept_name from procedure_occurrence po join concept c on c.concept_id = po.procedure_concept_id group by procedure_concept_id, c.concept_name order by count(*) desc",

                          "drug_concepts_count": "select count(distinct person_id) as n, count(*) as n_r, drug_concept_id, c.concept_name from drug_exposure de join concept c on c.concept_id = de.drug_concept_id group by drug_concept_id, c.concept_name order by count(*) desc",

                          "measurement_concepts_count": "select count(distinct person_id) as n, count(*) as n_r, measurement_concept_id, c.concept_name as measurement_concept_name, "
                                                        "min(value_as_number) as min_value_as_number, max(value_as_number) as max_value_as_number from measurement m join concept c on c.concept_id = m.measurement_concept_id group by measurement_concept_id, c.concept_name order by count(*) desc",

                          "observation_concepts_count": "select count(distinct person_id) as n, count(*) as n_r, observation_concept_id, concept_name from observation o join concept c on o.observation_concept_id = c.concept_id group by concept_name, observation_concept_id order by count(*) desc",

                          "device_concepts_count": "select count(distinct person_id) as n, count(*) as n_r, device_concept_id, c.concept_name from device_exposure de join concept c on c.concept_id = de.device_concept_id group by device_concept_id, c.concept_name order by count(*) desc",

                          "payer_concepts_count": "select count(distinct person_id) as n, count(*) as n_r, payer_concept_id, c.concept_name from payer_plan_period pp join concept c on c.concept_id = pp.payer_concept_id group by payer_concept_id, c.concept_name order by count(*) desc",

                          "locations_count": "select count(*) as n_r from location",

                          "provider_count": "select count(*) as n_r from provider",

                          "top_locations": "select count(*) as n_r, state, county, city  from location group by state, county, city order by count(*) desc"

                          }

    for tag in statistics_queries:

        print(f"{tag}:")
        query = statistics_queries[tag]
        print(query)
        print(spark.sql(query).toPandas())
        print("")

if __name__ == "__main__":

    arg_parser_obj = argparse.ArgumentParser(description="Stage CSV files that confirm to the PreparedSource format for mapping to OHDSI")
    arg_parser_obj.add_argument("-c", "--config-json-file-name", dest="config_json_file_name", default="/root/config/prepared_source_to_ohdsi_config.json.generated.parquet.json")
    arg_parser_obj.add_argument("-l", "--run-local", dest="run_local", default=False, action="store_true")

    arg_obj = arg_parser_obj.parse_args()

    spark = pyspark.sql.SparkSession.builder \
        .config("spark.driver.memory", "16g") \
        .getOrCreate()

    with open(arg_obj.config_json_file_name) as f:
        tbs = json.load(f)

    RUN_LOCAL = arg_obj.run_local

    if RUN_LOCAL:
        spark = SparkSession.builder.appName("StageStreamlinedPreparedSource") \
            .config('spark.driver.extraJavaOptions', '-Duser.timezone=GMT') \
            .config('spark.executor.extraJavaOptions', '-Duser.timezone=GMT') \
            .config('spark.sql.session.timeZone', 'UTC') \
            .config("spark.driver.memory", "16g") \
            .getOrCreate()
    else:
        spark = SparkSession.builder.appName("StageStreamlinedPreparedSource") \
            .config('spark.driver.extraJavaOptions', '-Duser.timezone=GMT') \
            .config('spark.executor.extraJavaOptions', '-Duser.timezone=GMT') \
            .config('spark.sql.session.timeZone', 'UTC') \
            .getOrCreate()

    with open(arg_obj.config_json_file_name, mode="r") as f:
        config = json.load(f)

    main(spark, config)