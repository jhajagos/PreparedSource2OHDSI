from pyspark.sql import SparkSession
from pyspark import SparkConf
import preparedsource2ohdsi.mapping_utilities as mu
import json
import argparse
import pprint
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', None)

def main(spark, tbs, extended_queries):

    catalog = mu.attach_catalog_dict(spark, tbs)

    statistics_queries = {"count_people": "select count(distinct person_id) as n, count(*) as n_r from person",
                          "count_visits": "with pc as (select count(distinct person_id) as p_n from person) select t.*, pc.* from  pc cross join (select count(distinct person_id) as n, count(*) as n_r, count(distinct visit_occurrence_id) as n_visit_occurrence_id from visit_occurrence) t",
                          "count_deaths": "with pc as (select count(distinct person_id) as p_n from person) select t.*, pc.* from pc cross join (select count(distinct person_id) as n, count(*) as n_r from death) t",
                          "count_observation_periods": "with pc as (select count(distinct person_id) as p_n from person) select t.*, pc.* from pc cross join  (select count(distinct person_id) as n, count(*) as n_r, count(distinct observation_period_id) as n_observation_period_id from observation_period) t",

                          "count_gender": "with pc as (select count(distinct person_id) as p_n from person) select t.*, pc.* from  pc cross join (select count(distinct person_id) as n, count(*) as n_r, concept_name as gender_concept_name from person p join concept c on p.gender_concept_id = c.concept_id group by gender_concept_id, concept_name) t order by n_r desc",
                          "count_race": "with pc as (select count(distinct person_id) as p_n from person) select t.*, pc.* from pc cross join (select count(distinct person_id) as n, count(*) as n_r, concept_name as race_concept_name from person p join concept c on  p.race_concept_id = c.concept_id group by race_concept_id, concept_name) t order by n_r desc",
                          "count_ethnicity": "with pc as (select count(distinct person_id) as p_n from person) select t.*, pc.* from pc cross join (select count(distinct person_id) as n, count(*) as n_r, concept_name as ethnicity_concept_name from person p join concept c on  p.ethnicity_concept_id = c.concept_id group by ethnicity_concept_id, concept_name) t order by n_r desc",

                          "count_conditions": "with pc as (select count(distinct person_id) as p_n from person) select t.*, pc.* from  pc cross join (select count(distinct person_id) as n, count(distinct visit_occurrence_id) as n_visits, count(*) as n_r,  count(distinct condition_occurrence_id) as n_condition_occurrence_id from condition_occurrence) t",
                          "count_procedures": "with pc as (select count(distinct person_id) as p_n from person) select t.*, pc.* from  pc cross join (select count(distinct person_id) as n,  count(distinct visit_occurrence_id) as n_visits, count(*) as n_r, count(distinct procedure_occurrence_id) as n_procedure_occurrence_id  from procedure_occurrence) t",

                          "count_measurements": "with pc as (select count(distinct person_id) as p_n from person) select t.*, pc.* from  pc cross join (select count(distinct person_id) as n, count(distinct visit_occurrence_id) as n_visits, count(*) as n_r, count(distinct measurement_id) as n_measurement_id from measurement) t",
                          "count_observations": "with pc as (select count(distinct person_id) as p_n from person) select t.*, pc.* from  pc cross join (select count(distinct person_id) as n, count(distinct visit_occurrence_id) as n_visits, count(*) as n_r, count(distinct observation_id) as n_observation_id from observation) t",

                          "count_drugs": "with pc as (select count(distinct person_id) as p_n from person) select t.*, pc.* from  pc cross join (select count(distinct person_id) as n, count(distinct visit_occurrence_id) as n_visits, count(*) as n_r, count(distinct drug_exposure_id) as n_drug_expousure_id from drug_exposure) t",
                          "count_devices": "with pc as (select count(distinct person_id) as p_n from person) select t.*, pc.* from  pc cross join (select count(distinct person_id) as n, count(distinct visit_occurrence_id) as n_visits, count(*) as n_r, count(distinct device_exposure_id) as d_device_exposure_id from device_exposure) t",

                          "count_payers": "with pc as (select count(distinct person_id) as p_n from person) select t.*, pc.* from  pc cross join (select count(distinct person_id) as n, count(*) as n_r, count(distinct payer_plan_period_id)  as n_payer_plan_period_id from payer_plan_period) t",

                          "count_notes": "with pc as (select count(distinct person_id) as p_n from person) select t.*, pc.* from  pc cross join (select count(distinct person_id) as n, count(distinct visit_occurrence_id) as n_visits, count(*) as n_r, count(distinct note_class_concept_id)  as n_note_class_concept_id from note) t",

                          "count_visit_concepts_count": "select count(distinct person_id) as n, count(*) as n_r, visit_concept_id, c.concept_name as visit_concept_name from visit_occurrence vo join concept c on vo.visit_concept_id = c.concept_id group by visit_concept_id, c.concept_name order by count(*) desc",

                          "condition_concepts_count": "select count(distinct person_id) as n, count(*) as n_r, condition_concept_id, c.concept_name as condition_concept_name from condition_occurrence co join concept c on c.concept_id = co.condition_concept_id group by condition_concept_id, c.concept_name order by count(*) desc",

                          "procedure_concepts_count": "select count(distinct person_id) as n, count(*) as n_r, procedure_concept_id, c.concept_name as procedure_concept_name from procedure_occurrence po join concept c on c.concept_id = po.procedure_concept_id group by procedure_concept_id, c.concept_name order by count(*) desc",

                          "drug_concepts_count": "select count(distinct person_id) as n, count(*) as n_r, drug_concept_id, c.concept_name from drug_exposure de join concept c on c.concept_id = de.drug_concept_id group by drug_concept_id, c.concept_name order by count(*) desc",

                          "measurement_concepts_count": "select count(distinct person_id) as n, count(*) as n_r, measurement_concept_id, c.concept_name as measurement_concept_name, "
                                                        "min(value_as_number) as min_value_as_number, max(value_as_number) as max_value_as_number, c2.concept_code as unit_concept_code, "
                                                        " min(measurement_date) as min_measurement_date, max(measurement_date) as max_measurement_date, percentile(value_as_number, 0.25) as p25, "
                                                         "percentile(value_as_number, 0.5) as p50, percentile(value_as_number, 0.75) as p75 " 
                                                        "from measurement m join concept c on c.concept_id = m.measurement_concept_id  "
                                                        "left outer join concept c2 on c2.concept_id = m.unit_concept_id group by measurement_concept_id, c.concept_name, c2.concept_code order by count(1) desc",

                          "observation_concepts_count": "select count(distinct person_id) as n, count(*) as n_r, observation_concept_id, concept_name from observation o join concept c on o.observation_concept_id = c.concept_id group by concept_name, observation_concept_id order by count(*) desc",

                          "device_concepts_count": "select count(distinct person_id) as n, count(*) as n_r, device_concept_id, c.concept_name from device_exposure de join concept c on c.concept_id = de.device_concept_id group by device_concept_id, c.concept_name order by count(*) desc",

                          "payer_concepts_count": "select count(distinct person_id) as n, count(*) as n_r, payer_concept_id, c.concept_name from payer_plan_period pp join concept c on c.concept_id = pp.payer_concept_id group by payer_concept_id, c.concept_name order by count(*) desc",

                          "note_concepts_count": "select count(*) as n_r, count(distinct person_id) as n, note_class_concept_id, c1.concept_name from note n join concept c1 on n.note_class_concept_id = c1.concept_id group by note_class_concept_id, c1.concept_name order by count(*) desc",

                          "locations_count": "select count(*) as n_r, count(distinct location_id)  as n_location_id from location",

                          "provider_count": "select count(*) as n_r, count(distinct provider_id) as n_provider_id from provider",

                          "top_locations": "select count(*) as n_r, state, county, city  from location group by state, county, city order by count(*) desc",

                          "care_site_count": "select count(*) as n_r, count(distinct care_site_id) as n_care_site_id from care_site",

                          "cdm_source": "select * from cdm_source"
                          }

    # More detailed queries
    extended_queries_to_run = {
        "yearly_visit_counts": "select count(distinct person_id) as n, count(distinct visit_occurrence_id) as n_visits, visit_year from (select  person_id, visit_occurrence_id, extract(year from visit_start_date) as visit_year from visit_occurrence) t group by visit_year order by visit_year desc",
        "yearly_condition_counts": "select count(distinct person_id) as n, count(distinct visit_occurrence_id) as n_visits, count(1) as n_r, condition_year from (select  person_id, visit_occurrence_id, extract(year from condition_start_date) as condition_year from condition_occurrence) t group by condition_year order by condition_year desc",
        "yearly_procedure_counts": "select count(distinct person_id) as n, count(distinct visit_occurrence_id) as n_visits, count(1) as n_r, procedure_year from (select  person_id, visit_occurrence_id, extract(year from procedure_date) as procedure_year from procedure_occurrence) t group by procedure_year order by procedure_year desc",
        "yearly_drug_counts": "select count(distinct person_id) as n, count(distinct visit_occurrence_id) as n_visits, count(1) as n_r, drug_year from (select  person_id, visit_occurrence_id, extract(year from drug_exposure_start_date) as drug_year from drug_exposure) t group by drug_year order by drug_year desc",
        "yearly_observation_counts": "select count(distinct person_id) as n, count(distinct visit_occurrence_id) as n_visits, count(1) as n_r, t.observation_year from (select  person_id, visit_occurrence_id, extract(year from observation_date) as observation_year from observation) t group by observation_year order by observation_year desc",
        "yearly_measurement_counts": "select count(distinct person_id) as n, count(distinct visit_occurrence_id) as n_visits, count(1) as n_r, t.measurement_year from (select  person_id, visit_occurrence_id, extract(year from measurement_date) as measurement_year from measurement) t group by measurement_year order by measurement_year desc",
        "yearly_device_counts": "select count(distinct person_id) as n, count(distinct visit_occurrence_id) as n_visits, count(1) as n_r, device_year from (select  person_id, visit_occurrence_id, extract(year from device_exposure_start_date) as device_year from device_exposure) t group by device_year order by device_year desc",
        "drugs_not_mapped_to_standard_concepts": "select t.*, c1.concept_code, c1.vocabulary_id, c1.concept_class_id, c1.concept_name from (select drug_source_value, drug_source_concept_id, count(*) as n_r, count(distinct person_id) as n from drug_exposure where drug_concept_id = 0 group by drug_source_value,drug_source_concept_id) t join concept c1 on c1.concept_id = t.drug_source_concept_id order by n_r desc"
    }

    if extended_queries is True:
        extended_queries_to_run.update(statistics_queries)
        queries_to_run = extended_queries_to_run
    else:
        queries_to_run = statistics_queries

    for tag in queries_to_run:
        print(f"{tag}:")
        query = queries_to_run[tag]
        print(query)
        q_df = spark.sql(query).toPandas()

        q_columns = q_df.columns
        if "p_n" in q_columns and "n" in q_columns:
            q_df["n / p_n"] = q_df["n"] / q_df["p_n"]

        print(q_df)
        print("")

if __name__ == "__main__":

    arg_parser_obj = argparse.ArgumentParser(description="Stage CSV files that confirm to the PreparedSource format for mapping to OHDSI")
    arg_parser_obj.add_argument("-c", "--config-json-file-name", dest="config_json_file_name", default="/root/config/prepared_source_to_ohdsi_config.json.generated.parquet.json")
    arg_parser_obj.add_argument("-l", "--run-local", dest="run_local", default=False, action="store_true")
    arg_parser_obj.add_argument("--extended-queries", dest="extended_queries", default=False, action="store_true")
    arg_parser_obj.add_argument("--spark-config", dest="spark_config_file_name", default=None)

    arg_obj = arg_parser_obj.parse_args()

    RUN_LOCAL = arg_obj.run_local

    if arg_obj.spark_config_file_name is not None:
        with open(arg_obj.spark_config_file_name, "r") as f:
            extra_spark_configs = json.load(f)
    else:
        extra_spark_configs = {}

    sconf = SparkConf()
    default_spark_conf_dict = {
        "spark.driver.extraJavaOptions": "-Duser.timezone=GMT",
        "spark.executor.extraJavaOptions": "-Duser.timezone=GMT",
        "spark.sql.session.timeZone": "UTC",
    }

    if RUN_LOCAL:
        default_spark_conf_dict["spark.driver.memory"] =  "16g"
    else:
        pass

    for key in extra_spark_configs:
        if key in default_spark_conf_dict:
            if key == "spark.driver.extraJavaOptions":
                default_spark_conf_dict[key] += f" {extra_spark_configs[key]}"
            elif key == "spark.executor.extraJavaOptions":
                default_spark_conf_dict[key] += f" {extra_spark_configs[key]}"
            else:
                default_spark_conf_dict[key] = extra_spark_configs[key]
        else:
            default_spark_conf_dict[key] = extra_spark_configs[key]

    print("Spark Configuration:")
    pprint.pprint(default_spark_conf_dict)

    for key in default_spark_conf_dict:
        sconf.set(key, default_spark_conf_dict[key])

    if RUN_LOCAL:
        sconf.setMaster("local[*]")

    spark = SparkSession.builder.config(conf=sconf).appName("BasicStatistics").getOrCreate()

    with open(arg_obj.config_json_file_name) as f:
        config = json.load(f)

    main(spark, config, extended_queries=arg_obj.extended_queries)