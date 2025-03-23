import datetime
import json
import logging
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import argparse
import os
import pprint
import time
import preparedsource2ohdsi.mapping_utilities as mapping_utilities

logging.basicConfig(level=logging.INFO)


# TODO: Stable patient_id(s) and visit_occurrence_id(s)

# TODO: Handle DX codes that map to measurement domain but have abnormal results:

# For example, ICD10 CONCEPT_ID 45548980 ‘Abnormal level of unspecified serum enzyme’ indicates a Measurement and the result (abnormal).
# In those situations, the CONCEPT_RELATIONSHIP table in addition to the ‘Maps to’ record contains a second record with
# the relationship_id set to ‘Maps to value’. In this example, the ‘Maps to’ relationship directs to 4046263 ‘Enzyme measurement’
# as well as a ‘Maps to value’ record to 4135493 ‘Abnormal’.

# TODO: Add provider links to other tables than visit_occurrence

# TODO: Allow prepared source tables to be processed and defined

CHECK_POINTING = 'LOCAL' #  BY default checkpointing is 'LOCAL' other option are ('REMOTE', 'NONE') this can be overwritten in the configuration file


def main(config, compute_data_checks=False, evaluate_samples=True, export_json_file_name=None, ohdsi_version=None,
         write_cdm_source=True):

    output_path = config["ohdsi_output_location"]

    logging.info(f"Check pointing mode: {CHECK_POINTING}")

    starting_time = time.time()

    if CHECK_POINTING == "REMOTE":
        check_point_path = output_path + "checkpoint/"

        logging.info(f"Check pointing directory: {check_point_path}")
        spark.sparkContext.setCheckpointDir(check_point_path)

    if "local_csv_output_path" in config: # For sample output
        local_path = config["local_csv_output_path"]

    # Get Concept Tables Needed for mapping
    concept_map_load_start_time = time.time()
    concept_map_sdf_dict = mapping_utilities.load_parquet_from_table_names(spark, ["concept", "concept_map", "vocabulary"], config["export_concept_mapping_table_path"], cache=True)
    concept_map_load_end_time = time.time()
    logging.info(f"Total time to load concept and concept_map: {format_log_time(concept_map_load_start_time, concept_map_load_end_time)}")

    # Get Prepared Source Tables
    prepared_source_table_names = [
        "source_care_site",
        "source_condition",
        "source_encounter",
        "source_encounter_detail",
        "source_medication",
        "source_observation_period",
        "source_person",
        "source_procedure",
        "source_device",
        "source_result",
        "source_location",
        "source_encounter_map",
        "source_person_map",
        "source_provider",
        "source_payer",
        "source_note"
    ]

    prepared_source_load_start_time = time.time()
    prepared_source_sdf_dict = mapping_utilities.load_parquet_from_table_names(spark, prepared_source_table_names,
                                                                               config["prepared_source_table_path"])
    prepared_source_load_end_time = time.time()
    logging.info(f"Total time to load prepared source tables: {prepared_source_load_end_time - prepared_source_load_start_time} seconds")

    concept_tables_names = [
        "concept",
        "concept_ancestor",
        "concept_relationship",
        "concept_synonym",
        "domain",
        "drug_strength",
        "vocabulary",
        "relationship",
        "concept_class"
    ]

    exported_table_dict = {}
    exported_table_dict["concept"] = {cn: generate_parquet_path(config["export_concept_mapping_table_path"], cn) for cn in concept_tables_names}
    exported_table_dict["prepared_source"] = {ps: generate_parquet_path(config["prepared_source_table_path"], ps) for ps in prepared_source_table_names}

    ohdsi_sdf_dict = {}  # Holds references to generated OHDSI tables
    source_person_sdf = prepared_source_sdf_dict["source_person"]
    source_person_sdf = filter_out_i_excluded(source_person_sdf)

    # Define variables for concept tables
    concept_sdf = concept_map_sdf_dict["concept"]
    concept_map_sdf = concept_map_sdf_dict["concept_map"]  # Generated with 'build_concept_tables_for_mapping.py'

    # Get mappings from OID to OHDSI Vocabulary IDs
    oid_to_vocab_id_path = os.path.join(os.path.dirname(__file__), "./mappings/oid_to_vocabulary_id.json")
    oid_to_vocab_sdf = mapping_utilities.json_dict_to_dataframe(spark,  oid_to_vocab_id_path,
                                                                ["oid", "vocabulary_id"]).cache()

    # Build Location table
    location_build_start_time = time.time()
    logging.info("Building location table")
    source_location_sdf = prepared_source_sdf_dict["source_location"]

    if ohdsi_version == "5.3.1":
        location_field_map = {
            "g_id": "location_id",
            "s_address_1": "address_1",
            "s_address_2": "address_2",
            "s_city": "city",
            "s_state": "state",
            "s_zip": "zip",
            "s_county": "county",
            "k_location": "location_source_value"
        }
    elif ohdsi_version == "5.4.1":
        location_field_map = {
            "g_id": "location_id",
            "s_address_1": "address_1",
            "s_address_2": "address_2",
            "s_city": "city",
            "s_state": "state",
            "s_zip": "zip",
            "s_county": "county",
            "s_country": "country_source_value",
            "s_latitude": "latitude",
            "s_longitude": "longitude",
            "k_location": "location_source_value"
        }

    ohdsi_location_sdf = mapping_utilities.map_table_column_names(source_location_sdf, location_field_map)
    ohdsi_location_sdf = mapping_utilities.column_names_to_align_to(ohdsi_location_sdf,
                                                                    ohdsi.LocationObject())

    ohdsi_location_sdf, location_path = mapping_utilities.write_parquet_file_and_reload(spark, ohdsi_location_sdf,
                                                                                          "location",
                                                                                          output_path)

    ohdsi_sdf_dict["location"] = ohdsi_location_sdf, location_path
    location_build_end_time = time.time()
    logging.info(f"Finished building location table (total elapsed time: {location_build_end_time - location_build_start_time} seconds)")
    if evaluate_samples:
        generate_local_samples(ohdsi_location_sdf, local_path, "location")

    # Build provider
    # TODO: Build out more provider fields, e.g., specialty
    logging.info("Building provider table")
    provider_build_start_time = time.time()

    source_provider_sdf = prepared_source_sdf_dict["source_provider"]

    provider_field_map = {
        "g_id": "provider_id",
        "s_provider_name": "provider_name",
        "k_provider": "provider_source_value",
        "s_npi": "npi"
    }

    ohdsi_provider_sdf = mapping_utilities.map_table_column_names(source_provider_sdf, provider_field_map)
    ohdsi_provider_sdf = mapping_utilities.column_names_to_align_to(ohdsi_provider_sdf, ohdsi.ProviderObject())

    ohdsi_provider_sdf, provider_path = mapping_utilities.write_parquet_file_and_reload(spark, ohdsi_provider_sdf, "provider", output_path)
    ohdsi_sdf_dict["provider"] = ohdsi_provider_sdf, provider_path

    provider_build_end_time = time.time()
    logging.info(
        f"Finished building provider table (total elapsed time: {provider_build_end_time - provider_build_start_time} seconds)")

    # Build person
    logging.info("Building person table")
    person_build_start_time = time.time()
    source_person_sdf = gender_code_mapper(source_person_sdf, concept_sdf, oid_to_vocab_sdf, concept_map_sdf)
    source_person_sdf = race_code_mapper(source_person_sdf, concept_sdf, oid_to_vocab_sdf, concept_map_sdf)
    source_person_sdf = ethnicity_code_mapper(source_person_sdf, concept_sdf, oid_to_vocab_sdf, concept_map_sdf)

    source_person_sdf = source_person_sdf.withColumn("g_birth_year", F.expr("extract(year from cast(s_birth_datetime as date))")). \
        withColumn("g_birth_month", F.expr("extract(month from cast(s_birth_datetime as date))")). \
        withColumn("g_birth_day", F.expr("extract(day from cast(s_birth_datetime as date))"))

    source_person_sdf = source_person_sdf.alias("p").join(ohdsi_location_sdf.alias("l"),
                                                                F.col("p.k_location") == F.col(
                                                                "l.location_source_value"), how="left_outer"). \
        select("p.*", F.col("l.location_id").alias("g_location_id"))

    # Todo: and g_source_table_name
    # Todo: pyspark.sql.functions.xxhash64 for hashing s_person_id
    patient_field_map = {
        "g_id": "person_id",
        "s_person_id": "person_source_value",
        "s_gender": "gender_source_value",
        "g_gender_source_concept_id": "gender_source_concept_id",
        "g_gender_concept_id": "gender_concept_id",
        "s_race": "race_source_value",
        "g_race_source_concept_id": "race_source_concept_id",
        "g_race_concept_id": "race_concept_id",
        "s_birth_datetime": "birth_datetime",
        "g_birth_year": "year_of_birth",
        "g_birth_month": "month_of_birth",
        "g_birth_day": "day_of_birth",
        "s_ethnicity": "ethnicity_source_value",
        "g_ethnicity_source_concept_id": "ethnicity_source_concept_id",
        "g_ethnicity_concept_id": "ethnicity_concept_id",
        "g_location_id": "location_id",
        "s_id": "s_id"
    }

    ohdsi_person_sdf = mapping_utilities.map_table_column_names(source_person_sdf, patient_field_map)
    ohdsi_person_sdf = mapping_utilities.column_names_to_align_to(ohdsi_person_sdf, ohdsi.PersonObject())

    ohdsi_person_sdf, person_path = mapping_utilities.write_parquet_file_and_reload(spark, ohdsi_person_sdf, "person", output_path)

    ohdsi_sdf_dict["person"] = ohdsi_person_sdf, person_path

    person_build_end_time = time.time()
    logging.info(f"Finished building person table (Total elapsed time: {format_log_time(person_build_start_time, person_build_end_time)})")

    if evaluate_samples:
        generate_local_samples(ohdsi_person_sdf, local_path,  "person")

    # Add death
    logging.info(f"Building death table")
    death_build_start_time = time.time()
    source_death_sdf = source_person_sdf.filter(F.col("s_death_datetime").isNotNull())

    source_death_sdf = death_source_code_mapper(source_death_sdf, concept_sdf, oid_to_vocab_sdf)
    source_death_sdf = source_death_sdf.withColumn("g_death_date", F.to_date(F.col("s_death_datetime")))

    death_field_map = {
        "g_id": "person_id",
        "s_death_datetime": "death_datetime",
        "g_death_date": "death_date",
        "g_death_type_concept_id": "death_type_concept_id"
    }

    source_death_sdf = mapping_utilities.map_table_column_names(source_death_sdf, death_field_map)
    ohdsi_death_sdf = mapping_utilities.column_names_to_align_to(source_death_sdf, ohdsi.DeathObject())

    ohdsi_death_sdf, death_path = mapping_utilities.write_parquet_file_and_reload(spark, ohdsi_death_sdf, "death",
                                                                                  output_path)

    death_build_end_time = time.time()
    logging.info(f"Finished building death table (Total elapsed time: {format_log_time(death_build_start_time, death_build_end_time)})")

    ohdsi_sdf_dict["death"] = ohdsi_death_sdf, death_path
    if evaluate_samples:
        generate_local_samples(ohdsi_death_sdf,  local_path, "death")

    # Generate care_site
    logging.info(f"Building care_site table")
    care_site_build_start_time = time.time()
    source_care_site_sdf = prepared_source_sdf_dict["source_care_site"]

    care_site_field_map = {"g_id": "care_site_id",
    "s_care_site_name": "care_site_name",
    "k_care_site": "care_site_source_value"}

    ohdsi_care_site_sdf = mapping_utilities.map_table_column_names(source_care_site_sdf, care_site_field_map)
    ohdsi_care_site_sdf = mapping_utilities.column_names_to_align_to(ohdsi_care_site_sdf, ohdsi.CareSiteObject())

    ohdsi_care_site_sdf, care_site_path = mapping_utilities.write_parquet_file_and_reload(spark, ohdsi_care_site_sdf, "care_site",
                                                        output_path)

    care_site_build_end_time = time.time()
    logging.info(f"Finished building care_site table (Total elapsed time: {format_log_time(care_site_build_start_time, care_site_build_end_time)})")

    ohdsi_sdf_dict["care_site"] = ohdsi_care_site_sdf, care_site_path
    if evaluate_samples:
        generate_local_samples(ohdsi_care_site_sdf, local_path, "care_site")

    # Generate visit_occurrence
    logging.info(f"Building visit_occurrence")
    visit_occurrence_build_start_time = time.time()
    source_encounter_sdf = prepared_source_sdf_dict["source_encounter"]
    source_encounter_sdf = filter_out_i_excluded(source_encounter_sdf)

    source_encounter_sdf = align_to_person_id(source_encounter_sdf, ohdsi_person_sdf)

    # Map whether it is an inpatient or outpatient visit
    source_encounter_sdf = visit_code_mapper(source_encounter_sdf, concept_sdf, oid_to_vocab_sdf, concept_map_sdf)

    # Map type of encounter
    source_encounter_sdf = visit_source_code_mapper(source_encounter_sdf, concept_sdf, oid_to_vocab_sdf)

    # Admit Source
    source_encounter_sdf = admit_source_code_mapper(source_encounter_sdf, concept_sdf, oid_to_vocab_sdf, concept_map_sdf)

    # Discharge To
    source_encounter_sdf = discharge_to_code_code_mapper(source_encounter_sdf, concept_sdf, oid_to_vocab_sdf, concept_map_sdf)

    source_encounter_sdf = source_encounter_sdf.withColumn("g_visit_start_date",
                                                           F.to_date(F.col("s_visit_start_datetime")))

    source_encounter_sdf = source_encounter_sdf.withColumn("g_visit_end_date", F.to_date(F.col("s_visit_end_datetime")))

    # Use the start date if there are no visit_end_date
    source_encounter_sdf = source_encounter_sdf.withColumn("g_visit_end_date", F.expr(
        "case when g_visit_end_date is null then g_visit_start_date else g_visit_end_date end"))

    # Add Care site
    source_encounter_sdf = source_encounter_sdf.alias("e").join(ohdsi_care_site_sdf.alias("c"),
                                                                F.col("e.k_care_site") == F.col("c.care_site_source_value"), how="left_outer").\
        select("e.*", F.col("c.care_site_id").alias("g_care_site_id"))

    source_encounter_sdf = source_encounter_sdf.alias("e").join(ohdsi_provider_sdf.alias("p"),
                                                          F.col("e.k_provider") == F.col(
                                                              "p.provider_source_value"), how="left_outer"). \
        select("e.*", F.col("p.provider_id").alias("g_provider_id"))

    # Todo: and g_source_table_name
    # Map fields for visit_occurrence_id
    if ohdsi_version == "5.3.1":
        visit_field_map = {
            "g_id": "visit_occurrence_id",
            "g_person_id": "person_id",
            "g_visit_concept_id": "visit_concept_id",
            "g_visit_source_concept_id": "visit_source_concept_id",
            "g_visit_type_concept_id": "visit_type_concept_id",
            "s_visit_start_datetime": "visit_start_datetime",
            "g_visit_start_date": "visit_start_date",
            "s_visit_end_datetime": "visit_end_datetime",
            "g_visit_end_date": "visit_end_date",
            "s_visit_type": "visit_source_value",
            "s_encounter_id": "s_encounter_id",
            "s_discharge_to": "discharge_to_source_value",
            "s_admitting_source": "admitting_source_value",
            "g_admitting_source_concept_id":  "admitting_source_concept_id",
            "g_discharge_to_concept_id": "discharge_to_concept_id",
            "s_person_id": "s_person_id",
            "g_care_site_id": "care_site_id",
            "g_provider_id": "provider_id",
            "s_id": "s_id"
        }
    elif ohdsi_version == "5.4.1":
        visit_field_map = {
            "g_id": "visit_occurrence_id",
            "g_person_id": "person_id",
            "g_visit_concept_id": "visit_concept_id",
            "g_visit_source_concept_id": "visit_source_concept_id",
            "g_visit_type_concept_id": "visit_type_concept_id",
            "s_visit_start_datetime": "visit_start_datetime",
            "g_visit_start_date": "visit_start_date",
            "s_visit_end_datetime": "visit_end_datetime",
            "g_visit_end_date": "visit_end_date",
            "s_visit_type": "visit_source_value",
            "s_encounter_id": "s_encounter_id",
            "s_admitting_source": "admitted_from_source_value",
            "g_admitting_source_concept_id": "admitted_from_source_concept_id",
            "s_discharge_to": "discharged_to_source_value",
            "g_discharge_to_concept_id": "discharged_to_concept_id",
            "s_person_id": "s_person_id",
            "g_care_site_id": "care_site_id",
            "g_provider_id": "provider_id",
            "s_id": "s_id"
        }

    ohdsi_visit_sdf = mapping_utilities.map_table_column_names(source_encounter_sdf, visit_field_map)

    # Build a support table for linking
    visit_source_link_sdf = ohdsi_visit_sdf.select("visit_occurrence_id", "s_encounter_id", "person_id", "s_person_id")
    visit_source_link_sdf, _ = mapping_utilities.write_parquet_file_and_reload(spark, visit_source_link_sdf, "visit_source_link",
                                                              output_path + "support/")

    ohdsi_visit_sdf = mapping_utilities.column_names_to_align_to(ohdsi_visit_sdf, ohdsi.VisitOccurrenceObject())

    ohdsi_visit_sdf, visit_path = mapping_utilities.write_parquet_file_and_reload(spark, ohdsi_visit_sdf, "visit_occurrence",
                                                                      output_path)

    visit_occurrence_build_end_time = time.time()
    logging.info(f"Finished building visit_occurrence (Total elapsed time: {format_log_time(visit_occurrence_build_start_time, visit_occurrence_build_end_time)})")

    ohdsi_sdf_dict["visit_occurrence"] = ohdsi_visit_sdf, visit_path

    if evaluate_samples:
        generate_local_samples(ohdsi_visit_sdf, local_path, "visit_occurrence")

    # Observation period
    logging.info("Building observation_period")
    observation_period_build_start_time = time.time()
    source_observation_period_sdf = prepared_source_sdf_dict["source_observation_period"]
    source_observation_period_sdf = align_to_person_id(source_observation_period_sdf, ohdsi_person_sdf)
    source_observation_period_sdf = observation_period_source_code_mapper(source_observation_period_sdf, concept_sdf, oid_to_vocab_sdf)

    source_observation_period_sdf = source_observation_period_sdf.withColumn("g_observation_period_start_date",
                                                                             F.to_date(F.col("s_start_observation_datetime"))).\
        withColumn("g_observation_period_end_date", F.to_date(F.col("s_end_observation_datetime")))

    observation_period_fields = {
        "g_id": "observation_period_id",
        "g_person_id": "person_id",
        "g_observation_period_start_date": "observation_period_start_date",
        "g_observation_period_end_date": "observation_period_end_date",
        "g_period_type_concept_id": "period_type_concept_id"
    }

    source_observation_period_sdf = mapping_utilities.map_table_column_names(source_observation_period_sdf, observation_period_fields)
    ohdsi_observation_period_sdf = mapping_utilities.column_names_to_align_to(source_observation_period_sdf, ohdsi.ObservationPeriodObject())

    ohdsi_observation_period_sdf, observation_path = mapping_utilities.write_parquet_file_and_reload(spark, ohdsi_observation_period_sdf, "observation_period",
                                                                          output_path)

    observation_period_build_end_time = time.time()
    logging.info(f"Finished building observation_period (Total elapsed time: f{format_log_time(observation_period_build_start_time, observation_period_build_end_time)})")

    ohdsi_sdf_dict["observation_period"] = ohdsi_observation_period_sdf, observation_path
    if evaluate_samples:
        generate_local_samples(ohdsi_observation_period_sdf, local_path, "observation_period")

    # Plan Provider
    logging.info("Building payer_plan_period")
    payer_plan_period_build_start_time = time.time()
    source_payer_sdf = prepared_source_sdf_dict["source_payer"]
    source_payer_sdf = align_to_person_id(source_payer_sdf, ohdsi_person_sdf)
    source_payer_sdf = payer_source_code_code_mapper(source_payer_sdf, concept_sdf, oid_to_vocab_sdf, concept_map_sdf)

    source_payer_sdf = source_payer_sdf.withColumn("g_payer_start_date", F.to_date(F.col("s_payer_start_datetime")))
    source_payer_sdf = source_payer_sdf.withColumn("g_payer_end_date", F.to_date(F.col("s_payer_end_datetime")))

    payer_plan_build_map = {
        "g_id": "payer_plan_period_id",
        "g_person_id": "person_id",
        "g_payer_start_date": "payer_plan_period_start_date",
        "g_payer_end_date": "payer_plan_period_end_date",
        "s_payer": "payer_source_value",
        "g_payer_concept_id": "payer_concept_id",
        "g_payer_source_concept_id": "payer_source_concept_id"
    }

    source_payer_sdf = mapping_utilities.map_table_column_names(source_payer_sdf, payer_plan_build_map)
    ohdsi_payer_plan_period_sdf = mapping_utilities.column_names_to_align_to(source_payer_sdf, ohdsi.PayerPlanPeriodObject())

    ohdsi_payer_plan_period_sdf, payer_plan_period_path = mapping_utilities.write_parquet_file_and_reload(spark, ohdsi_payer_plan_period_sdf, "payer_plan_period", output_path)

    ohdsi_sdf_dict["payer_plan_period"] = ohdsi_payer_plan_period_sdf, payer_plan_period_path

    payer_plan_period_build_end_time = time.time()
    logging.info(f"Finished building payer_plan_period (Total elapsed time: f{format_log_time(payer_plan_period_build_start_time, payer_plan_period_build_end_time)})")

    # Visit_Detail
    logging.info("Building visit_detail")
    visit_detail_build_start_time = time.time()
    source_encounter_detail_sdf = prepared_source_sdf_dict["source_encounter_detail"]
    source_encounter_detail_sdf = filter_out_i_excluded(source_encounter_detail_sdf)

    source_encounter_detail_sdf = align_to_visit(source_encounter_detail_sdf, ohdsi_person_sdf, visit_source_link_sdf)

    source_encounter_detail_sdf = source_encounter_detail_sdf.withColumn("g_visit_detail_start_date",
                                                           F.to_date(F.col("s_start_datetime")))

    source_encounter_detail_sdf = source_encounter_detail_sdf.withColumn("g_visit_detail_end_date", F.to_date(F.col("s_start_datetime")))

    # Use the start date if there are no visit_end_date
    source_encounter_detail_sdf = source_encounter_detail_sdf.withColumn("g_visit_detail_end_date", F.expr(
        "case when g_visit_detail_end_date is null then g_visit_detail_start_date else g_visit_detail_end_date end"))

    # Add Care site
    source_encounter_detail_sdf = source_encounter_detail_sdf.alias("e").join(ohdsi_care_site_sdf.alias("c"),
                                                                F.col("e.k_care_site") == F.col(
                                                                    "c.care_site_source_value"), how="left_outer"). \
        select("e.*", F.col("c.care_site_id").alias("g_care_site_id"))

    # Map whether it is an inpatient or outpatient visit detail
    source_encounter_detail_sdf = visit_detail_code_mapper(source_encounter_detail_sdf, concept_sdf, oid_to_vocab_sdf, concept_map_sdf)

    # Map type of encounter detail
    source_encounter_detail_sdf = visit_detail_source_code_mapper(source_encounter_detail_sdf, concept_sdf, oid_to_vocab_sdf)

    # Map fields to encounter
    visit_detail_field_map = {
        "g_id": "visit_detail_id",
        "g_person_id": "person_id",
        "g_visit_occurrence_id": "visit_occurrence_id",
        "g_visit_detail_concept_id": "visit_detail_concept_id",
        "g_visit_detail_source_concept_id": "visit_detail_source_concept_id",
        "g_visit_detail_type_concept_id": "visit_detail_type_concept_id",
        "s_start_datetime": "visit_detail_start_datetime",
        "g_visit_detail_start_date": "visit_detail_start_date",
        "s_end_datetime": "visit_detail_end_datetime",
        "g_visit_detail_end_date": "visit_detail_end_date",
        "s_visit_detail_type": "visit_detail_source_value",
        "s_person_id": "s_person_id",
        "g_care_site_id": "care_site_id"
    }
    ohdsi_visit_detail_sdf = mapping_utilities.map_table_column_names(source_encounter_detail_sdf, visit_detail_field_map)
    ohdsi_visit_detail_sdf = mapping_utilities.column_names_to_align_to(ohdsi_visit_detail_sdf,
                                                                        ohdsi.VisitDetailObject())

    ohdsi_visit_detail_sdf, visit_detail_path = mapping_utilities.write_parquet_file_and_reload(spark,
                                                                                                     ohdsi_visit_detail_sdf,
                                                                                                     "visit_detail",
                                                                                                     output_path)
    visit_detail_build_end_time = time.time()
    logging.info(f"Finished building visit_detail (Total elapsed time: {format_log_time(visit_detail_build_start_time, visit_detail_build_end_time)})")

    ohdsi_sdf_dict["visit_detail"] = ohdsi_visit_detail_sdf, visit_detail_path
    if evaluate_samples:
        generate_local_samples(ohdsi_observation_period_sdf, local_path, "visit_detail")

    # Procedure source_condition (generating domain source tables)
    logging.info("Started processing source_condition data (build domain mapped tables)")
    source_condition_process_start_time = time.time()
    source_condition_sdf = prepared_source_sdf_dict["source_condition"]
    source_condition_sdf = filter_out_i_excluded(source_condition_sdf)

    # Add visit_occurrence_id and person_id
    source_condition_sdf = align_to_visit(source_condition_sdf, ohdsi_person_sdf, visit_source_link_sdf)

    source_condition_sdf = condition_type_code_mapper(source_condition_sdf, concept_sdf, oid_to_vocab_sdf)
    source_condition_sdf = condition_status_code_mapper(source_condition_sdf, concept_sdf, oid_to_vocab_sdf, concept_map_sdf)

    # Add provider associated with the condition
    source_condition_sdf = source_condition_sdf.alias("c").join(ohdsi_provider_sdf.alias("p"),
                                                    F.col("c.k_provider") == F.col(
                                                        "p.provider_source_value"), how="left_outer"). \
        select("c.*", F.col("p.provider_id").alias("g_provider_id"))


    if CHECK_POINTING == "LOCAL":
        source_condition_sdf = source_condition_sdf.localCheckpoint()
    elif CHECK_POINTING == "REMOTE":
        source_condition_sdf = source_condition_sdf.checkpoint()
    else:
        pass

    # Determine the domain based on mapped concept_id's domain
    source_condition_matched_sdf = map_codes_to_domain(source_condition_sdf, concept_sdf, oid_to_vocab_sdf, concept_map_sdf,
                                                       "s_condition_code", "s_condition_code_type_oid", "g_condition_occurrence_id",
                                                       "source_condition",
                                                       join_type="left_outer")
    # Generate date fields
    source_condition_matched_sdf = source_condition_matched_sdf.withColumn("g_start_condition_date",
                                                                           F.to_date(F.col("s_start_condition_datetime")))
    source_condition_matched_sdf = source_condition_matched_sdf.withColumn("g_end_condition_date", F.to_date(
        F.col("s_end_condition_datetime")))

    # Write a parquet file partitioned by domain
    source_condition_matched_sdf = write_source_to_parquet_partitioned_by_domain(source_condition_matched_sdf,
                                                                                 output_path,
                                                                               "source_condition_matched")

    condition_domain_field_map = {
        "g_condition_occurrence_id": "condition_occurrence_id",
        "g_person_id": "person_id",
        "g_visit_occurrence_id": "visit_occurrence_id",
        "g_source_concept_id": "condition_source_concept_id",
        "g_concept_id": "condition_concept_id",
        "s_start_condition_datetime": "condition_start_datetime",
        "g_start_condition_date": "condition_start_date",
        "s_end_condition_datetime": "condition_end_datetime",
        "g_end_condition_date": "condition_end_date",
        "s_condition_code": "condition_source_value",
        "s_condition_status": "condition_status_source_value",
        "g_condition_type_concept_id": "condition_type_concept_id",
        "g_condition_status_concept_id": "condition_status_concept_id",
        "s_id": "s_id",
        "g_source_table_name": "g_source_table_name",
        "g_provider_id": "provider_id"
    }

    root_source_path = output_path + "source/"
    condition_source_path = root_source_path + "condition/"

    condition_domain_sdf = build_mapped_domain_df(spark, source_condition_matched_sdf,
                                                                   field_map=condition_domain_field_map,
                                                                   output_obj=ohdsi.ConditionOccurrenceObject(),
                                                                   domain="Condition",
                                                                   table_name="condition_occurrence",
                                                                   source_path=condition_source_path)

    # Holds a lists of data frames
    ohdsi_sdf_dict["condition_occurrence"] = []
    ohdsi_sdf_dict["procedure_occurrence"] = []
    ohdsi_sdf_dict["drug_exposure"] = []
    ohdsi_sdf_dict["device_exposure"] = []
    ohdsi_sdf_dict["measurement"] = []
    ohdsi_sdf_dict["observation"] = []

    primary_key_dict = {
        "procedure_occurrence": "procedure_occurrence_id",
        "measurement": "measurement_id",
        "drug_exposure": "drug_exposure_id",
        "device_exposure": "device_exposure_id",
        "observation": "observation_id",
        "condition_occurrence": "condition_occurrence_id"
    }

    ohdsi_sdf_dict["condition_occurrence"] += [condition_domain_sdf]

    if evaluate_samples:
        generate_local_samples(condition_domain_sdf, local_path, "condition_occurrence")

    if ohdsi_version == "5.3.1":
        procedure_domain_condition_source_field_map = {
            "g_condition_occurrence_id": "procedure_occurrence_id",
            "g_person_id": "person_id",
            "g_visit_occurrence_id": "visit_occurrence_id",
            "g_source_concept_id": "procedure_source_concept_id",
            "g_concept_id": "procedure_concept_id",
            "s_start_condition_datetime": "procedure_datetime",
            "g_start_condition_date": "procedure_date",
            "s_end_condition_datetime": "procedure_end_datetime",
            "g_end_condition_date": "procedure_end_date",
            "s_condition_code": "procedure_source_value",
            "g_condition_type_concept_id": "procedure_type_concept_id",
            "s_id": "s_id",
            "g_source_table_name": "g_source_table_name"
        }
    elif ohdsi_version == "5.4.1":
        procedure_domain_condition_source_field_map = {
            "g_condition_occurrence_id": "procedure_occurrence_id",
            "g_person_id": "person_id",
            "g_visit_occurrence_id": "visit_occurrence_id",
            "g_source_concept_id": "procedure_source_concept_id",
            "g_concept_id": "procedure_concept_id",
            "s_start_condition_datetime": "procedure_datetime",
            "g_start_condition_date": "procedure_date",
            "s_end_condition_datetime": "procedure_end_datetime",
            "g_end_condition_date": "procedure_end_date",
            "s_condition_code": "procedure_source_value",
            "g_condition_type_concept_id": "procedure_type_concept_id",
            "s_id": "s_id",
            "g_source_table_name": "g_source_table_name"
        }

    procedure_domain_condition_source_sdf = build_mapped_domain_df(spark, source_condition_matched_sdf,
                                                                     field_map=procedure_domain_condition_source_field_map,
                                                                     output_obj=ohdsi.ProcedureOccurrenceObject(),
                                                                     domain="Procedure",
                                                                     table_name="procedure_occurrence",
                                                                     source_path=condition_source_path)

    ohdsi_sdf_dict["procedure_occurrence"] += [procedure_domain_condition_source_sdf]

    if evaluate_samples:
        generate_local_samples(procedure_domain_condition_source_sdf, local_path, "procedure_occurrence_source_condition")

    observation_domain_condition_source_field_map = {
        "g_condition_occurrence_id": "observation_id",
        "g_person_id": "person_id",
        "g_visit_occurrence_id": "visit_occurrence_id",
        "g_source_concept_id": "observation_source_concept_id",
        "g_concept_id": "observation_concept_id",
        "s_start_condition_datetime": "observation_datetime",
        "g_start_condition_date": "observation_date",
        "s_condition_code": "observation_source_value",
        "g_condition_type_concept_id": "observation_type_concept_id",
        "s_id": "s_id",
        "g_source_table_name": "g_source_table_name"
    }

    observation_domain_condition_source_sdf = build_mapped_domain_df(spark, source_condition_matched_sdf,
                                                                     field_map=observation_domain_condition_source_field_map,
                                                                     output_obj=ohdsi.ObservationObject(),
                                                                     domain="Observation",
                                                                     table_name="observation",
                                                                     source_path=condition_source_path)

    ohdsi_sdf_dict["observation"] += [observation_domain_condition_source_sdf]

    if evaluate_samples:
        generate_local_samples(observation_domain_condition_source_sdf, local_path, "observation_source_condition")

    measurement_domain_condition_source_field_map = {
        "g_condition_occurrence_id": "measurement_id",
        "g_person_id": "person_id",
        "g_visit_occurrence_id": "visit_occurrence_id",
        "g_source_concept_id": "measurement_source_concept_id",
        "g_concept_id": "measurement_concept_id",
        "s_start_condition_datetime": "measurement_datetime",
        "g_start_condition_date": "measurement_date",
        "s_condition_code": "measurement_source_value",
        "g_condition_type_concept_id": "measurement_type_concept_id",
        "s_id": "s_id",
        "g_source_table_name": "g_source_table_name"
    }

    measurement_domain_condition_source_sdf = build_mapped_domain_df(spark, source_condition_matched_sdf,
                                                  field_map=measurement_domain_condition_source_field_map,
                                                  output_obj=ohdsi.MeasurementObject(),
                                                  domain="Measurement",
                                                  table_name="measurement",
                                                  source_path=condition_source_path)

    ohdsi_sdf_dict["measurement"] += [measurement_domain_condition_source_sdf]

    source_condition_process_end_time = time.time()
    logging.info(f"Finished processing source_condition (Total elapsed time: {format_log_time(source_condition_process_start_time, source_condition_process_end_time)})")

    if evaluate_samples:
        generate_local_samples(measurement_domain_condition_source_sdf, local_path, "measurement_source_condition.csv")

    # Process source_procedure
    logging.info("Started processing source_procedure")
    source_procedure_process_start_time = time.time()
    source_procedure_sdf = prepared_source_sdf_dict["source_procedure"]
    source_procedure_sdf = filter_out_i_excluded(source_procedure_sdf)
    source_procedure_sdf = align_to_visit(source_procedure_sdf, ohdsi_person_sdf, visit_source_link_sdf)

    source_procedure_sdf = source_procedure_sdf.withColumn("g_start_procedure_date", F.to_date("s_start_procedure_datetime"))
    if ohdsi_version == "5.3.1":
        pass
    elif ohdsi_version == "5.4.1":
        source_procedure_sdf = source_procedure_sdf.withColumn("g_end_procedure_date", F.to_date("s_end_procedure_datetime"))

    source_procedure_sdf = procedure_type_code_mapper(source_procedure_sdf, concept_sdf, oid_to_vocab_sdf)

    # Add provider associated with procedure
    source_procedure_sdf = source_procedure_sdf.alias("pc").join(ohdsi_provider_sdf.alias("p"),
                                                                F.col("pc.k_provider") == F.col(
                                                                    "p.provider_source_value"), how="left_outer"). \
        select("pc.*", F.col("p.provider_id").alias("g_provider_id"))

    source_procedure_matched_sdf = map_codes_to_domain(source_procedure_sdf, concept_sdf, oid_to_vocab_sdf,
                                                       concept_map_sdf,
                                                       "s_procedure_code", "s_procedure_code_type_oid",
                                                       "g_procedure_occurrence_id",
                                                       "source_procedure",
                                                       join_type="left_outer")

    # Write a parquet file partitioned by domain
    source_procedure_matched_sdf = write_source_to_parquet_partitioned_by_domain(source_procedure_matched_sdf,
                                                                                 output_path,
                                                                                  "source_procedure_matched")
    if ohdsi_version == "5.3.1":
        procedure_field_map = {
            "g_procedure_occurrence_id": "procedure_occurrence_id",
            "g_person_id": "person_id",
            "g_visit_occurrence_id": "visit_occurrence_id",
            "g_source_concept_id": "procedure_source_concept_id",
            "g_concept_id": "procedure_concept_id",
            "s_start_procedure_datetime": "procedure_datetime",
            "g_start_procedure_date": "procedure_date",
            "s_procedure_code": "procedure_source_value",
            "g_procedure_type_concept_id": "procedure_type_concept_id",
            "s_id": "s_id",
            "g_source_table_name": "g_source_table_name",
            "g_provider_id": "provider_id"
        }
    elif ohdsi_version == "5.4.1":
        procedure_field_map = {
            "g_procedure_occurrence_id": "procedure_occurrence_id",
            "g_person_id": "person_id",
            "g_visit_occurrence_id": "visit_occurrence_id",
            "g_source_concept_id": "procedure_source_concept_id",
            "g_concept_id": "procedure_concept_id",
            "s_start_procedure_datetime": "procedure_datetime",
            "g_start_procedure_date": "procedure_date",
            "s_end_procedure_datetime": "procedure_end_datetime",
            "g_end_procedure_date": "procedure_end_date",
            "s_procedure_code": "procedure_source_value",
            "g_procedure_type_concept_id": "procedure_type_concept_id",
            "s_id": "s_id",
            "g_source_table_name": "g_source_table_name",
            "g_provider_id": "provider_id"
        }

    procedure_source_path = root_source_path + "procedure/"

    procedure_source_sdf = build_mapped_domain_df(spark, source_procedure_matched_sdf,
                                                              field_map=procedure_field_map,
                                                              output_obj=ohdsi.ProcedureOccurrenceObject(),
                                                              domain="Procedure",
                                                              table_name="procedure_occurrence",
                                                              source_path=procedure_source_path)

    ohdsi_sdf_dict["procedure_occurrence"] += [procedure_source_sdf]

    if evaluate_samples:
        generate_local_samples(procedure_source_sdf, local_path, "procedure_occurrence")

    # Source Procedure -> domain/mapped_domain Drug
    # TODO:  drug_exposure_end_date cannot be null
    if ohdsi_version == "5.3.1":
        drug_domain_procedure_source_field_map = {
            "g_procedure_occurrence_id": "drug_exposure_id",
            "g_person_id": "person_id",
            "g_visit_occurrence_id": "visit_occurrence_id",
            "g_source_concept_id": "drug_source_concept_id",
            "g_concept_id": "drug_concept_id",
            "s_start_procedure_datetime": "drug_exposure_start_datetime",
            "g_start_procedure_date": "drug_exposure_start_date",
            "s_procedure_code": "drug_source_value",
            "g_procedure_type_concept_id": "drug_type_concept_id",
            "s_id": "s_id",
            "g_source_table_name": "g_source_table_name"
        }
    elif ohdsi_version == "5.4.1":
        drug_domain_procedure_source_field_map = {
            "g_procedure_occurrence_id": "drug_exposure_id",
            "g_person_id": "person_id",
            "g_visit_occurrence_id": "visit_occurrence_id",
            "g_source_concept_id": "drug_source_concept_id",
            "g_concept_id": "drug_concept_id",
            "s_start_procedure_datetime": "drug_exposure_start_datetime",
            "g_start_procedure_date": "drug_exposure_start_date",
            "s_end_procedure_datetime": "drug_exposure_end_datetime",
            "g_end_procedure_date": "drug_exposure_end_date",
            "s_procedure_code": "drug_source_value",
            "g_procedure_type_concept_id": "drug_type_concept_id",
            "s_id": "s_id",
            "g_source_table_name": "g_source_table_name"
        }

    drug_domain_procedure_source_sdf = build_mapped_domain_df(spark, source_procedure_matched_sdf,
                                                                     field_map=drug_domain_procedure_source_field_map,
                                                                     output_obj=ohdsi.DrugExposureObject(),
                                                                     domain="Drug",
                                                                     table_name="drug_exposure",
                                                                     source_path=procedure_source_path)

    if evaluate_samples:
        generate_local_samples(drug_domain_procedure_source_sdf, local_path, "drug_exposure_source_procedure")

    ohdsi_sdf_dict["drug_exposure"] += [drug_domain_procedure_source_sdf]

    measurement_domain_procedure_source_field_map = {
        "g_procedure_occurrence_id": "measurement_id",
        "g_person_id": "person_id",
        "g_visit_occurrence_id": "visit_occurrence_id",
        "g_source_concept_id": "measurement_source_concept_id",
        "g_concept_id": "measurement_concept_id",
        "s_start_procedure_datetime": "measurement_datetime",
        "g_start_procedure_date": "measurement_date",
        "s_procedure_code": "measurement_source_value",
        "g_procedure_type_concept_id": "measurement_type_concept_id",
        "s_id": "s_id",
        "g_source_table_name": "g_source_table_name"
    }

    measurement_domain_procedure_source_sdf = build_mapped_domain_df(spark, source_procedure_matched_sdf,
                                                                     field_map=measurement_domain_procedure_source_field_map,
                                                                     output_obj=ohdsi.MeasurementObject(),
                                                                     domain="Measurement",
                                                                     table_name="measurement",
                                                                     source_path=procedure_source_path)

    ohdsi_sdf_dict["measurement"] += [measurement_domain_procedure_source_sdf]

    if evaluate_samples:
        generate_local_samples(measurement_domain_procedure_source_sdf, local_path, "measurement_source_procedure")

    observation_domain_procedure_source_field_map = {
        "g_procedure_occurrence_id": "observation_id",
        "g_person_id": "person_id",
        "g_visit_occurrence_id": "visit_occurrence_id",
        "g_source_concept_id": "observation_source_concept_id",
        "g_concept_id": "observation_concept_id",
        "s_start_procedure_datetime": "observation_datetime",
        "g_start_procedure_date": "observation_date",
        "s_procedure_code": "observation_source_value",
        "g_procedure_type_concept_id": "observation_type_concept_id",
        "s_id": "s_id",
        "g_source_table_name": "g_source_table_name"
    }

    observation_domain_procedure_source_sdf = build_mapped_domain_df(spark, source_procedure_matched_sdf,
                                                                     field_map=observation_domain_procedure_source_field_map,
                                                                     output_obj=ohdsi.ObservationObject(),
                                                                     domain="Observation",
                                                                     table_name="observation",
                                                                     source_path=procedure_source_path)

    ohdsi_sdf_dict["observation"] += [observation_domain_procedure_source_sdf]

    if evaluate_samples:
        generate_local_samples(observation_domain_procedure_source_sdf, local_path, "observation_source_procedure")

    if ohdsi_version == "5.3.1":
        device_domain_procedure_source_field_map = {
            "g_procedure_occurrence_id": "device_exposure_id",
            "g_person_id": "person_id",
            "g_visit_occurrence_id": "visit_occurrence_id",
            "g_source_concept_id": "device_source_concept_id",
            "g_concept_id": "device_concept_id",
            "s_start_procedure_datetime": "device_exposure_start_datetime",
            "g_start_procedure_date": "device_exposure_start_date",
            "s_procedure_code": "device_source_value",
            "g_procedure_type_concept_id": "device_type_concept_id",
            "s_id": "s_id",
            "g_source_table_name": "g_source_table_name"
        }
    elif ohdsi_version == "5.4.1":
        device_domain_procedure_source_field_map = {
            "g_procedure_occurrence_id": "device_exposure_id",
            "g_person_id": "person_id",
            "g_visit_occurrence_id": "visit_occurrence_id",
            "g_source_concept_id": "device_source_concept_id",
            "g_concept_id": "device_concept_id",
            "s_start_procedure_datetime": "device_exposure_start_datetime",
            "g_start_procedure_date": "device_exposure_start_date",
            "s_end_procedure_datetime": "device_exposure_end_datetime",
            "g_end_procedure_date": "device_exposure_end_date",
            "s_procedure_code": "device_source_value",
            "g_procedure_type_concept_id": "device_type_concept_id",
            "s_id": "s_id",
            "g_source_table_name": "g_source_table_name"
        }

    device_domain_procedure_source_sdf = build_mapped_domain_df(spark, source_procedure_matched_sdf,
                                                                     field_map=device_domain_procedure_source_field_map,
                                                                     output_obj=ohdsi.DeviceExposureObject(),
                                                                     domain="Device",
                                                                     table_name="device_exposure",
                                                                     source_path=procedure_source_path)

    ohdsi_sdf_dict["device_exposure"] += [device_domain_procedure_source_sdf]
    source_procedure_process_end_time = time.time()
    logging.info(f"Finished processing source_procedure (Total elapsed time: {format_log_time(source_procedure_process_start_time, source_procedure_process_end_time)})")

    if evaluate_samples:
        generate_local_samples(device_domain_procedure_source_sdf, local_path, "device_exposure_source_procedure")

    # Device_Exposure
    logging.info("Started building main device_exposure table")
    source_device_process_start_time = time.time()
    source_device_sdf = prepared_source_sdf_dict["source_device"]
    source_device_sdf = filter_out_i_excluded(source_device_sdf)
    source_device_sdf = align_to_visit(source_device_sdf, ohdsi_person_sdf, visit_source_link_sdf)

    source_device_sdf = source_device_sdf.withColumn("g_start_device_date", F.to_date("s_start_device_datetime"))
    source_device_sdf = source_device_sdf.withColumn("g_end_device_date", F.to_date("s_end_device_datetime"))
    source_device_sdf = device_type_code_mapper(source_device_sdf, concept_sdf, oid_to_vocab_sdf)

    #(source_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf, m_code, m_code_oid, s_code, s_code_oid,
    #                                       source_concept_id_field_name, concept_id_field_name, mapped_domain_id=None)
    source_device_matched_sdf = mapped_and_source_standard_code_mapper(source_device_sdf, concept_sdf, oid_to_vocab_sdf,
                                                       concept_map_sdf,
                                                       "m_device_code", "m_device_code_type_oid",
                                                       "s_device_code", "s_device_code_type_oid",
                                                       "g_device_source_concept_id",
                                                       "g_device_concept_id")

    source_device_matched_sdf = source_device_matched_sdf.withColumn("g_source_table_name", F.lit("source_device"))

    source_device_matched_sdf = source_device_matched_sdf.withColumn("mapped_domain_id",
                                                                     F.col("g_device_concept_id_domain_id"))

    source_device_matched_sdf = source_device_matched_sdf.withColumn("domain_id",
                                                                     F.col("g_device_concept_id_domain_id"))


    device_exposure_source_field_map = {
        "g_id": "device_exposure_id",
        "g_person_id": "person_id",
        "g_visit_occurrence_id": "visit_occurrence_id",
        "g_device_source_concept_id": "device_source_concept_id",
        "g_device_concept_id": "device_concept_id",
        "s_start_device_datetime": "device_exposure_start_datetime",
        "g_start_device_date": "device_exposure_start_date",
        "s_end_device_datetime": "device_exposure_end_datetime",
        "g_end_device_date": "device_exposure_end_date",
        "s_device_code": "device_source_value",
        "g_device_type_concept_id": "device_type_concept_id",
        "s_id": "s_id",
        "g_source_table_name": "g_source_table_name",
        "s_unique_device_identifier": "unique_device_id"
    }

    device_source_path = root_source_path + "device/"

    device_domain_device_source_sdf = build_mapped_domain_df(spark, source_device_matched_sdf,
                                                                field_map=device_exposure_source_field_map,
                                                                output_obj=ohdsi.DeviceExposureObject(),
                                                                domain="Device",
                                                                table_name="device_exposure",
                                                                source_path=device_source_path)

    ohdsi_sdf_dict["device_exposure"] += [device_domain_device_source_sdf]
    source_device_process_end_time = time.time()
    logging.info(
        f"Finished processing source_procedure (Total elapsed time: {format_log_time(source_device_process_start_time, source_device_process_end_time)})")

    # Main Drug Exposure
    logging.info("Started building main drug_exposure table")
    drug_exposure_build_start_time = time.time()
    source_med_sdf = prepared_source_sdf_dict["source_medication"]
    source_med_sdf = filter_out_i_excluded(source_med_sdf)
    source_med_sdf = align_to_visit(source_med_sdf, ohdsi_person_sdf, visit_source_link_sdf)

    source_med_sdf = source_med_sdf.withColumn("g_start_medication_date", F.to_date(F.col("s_start_medication_datetime")))
    source_med_sdf = source_med_sdf.withColumn("g_end_medication_date", F.to_date(F.col("s_end_medication_datetime")))

    source_med_sdf = source_med_sdf.withColumn("gg_drug_code", F.expr("coalesce(s_drug_code, m_drug_code, g_drug_code)"))
    source_med_sdf = source_med_sdf.withColumn("gg_drug_code_text", F.expr("coalesce(s_drug_text, s_drug_alternative_text, m_drug_text, g_drug_text)"))

    source_med_sdf = source_med_sdf.withColumn("g_drug_code_with_name", F.expr("gg_drug_code || '|' || gg_drug_code_text"))

    source_med_sdf = drug_type_code_mapper(source_med_sdf, concept_sdf, oid_to_vocab_sdf)
    source_med_sdf = route_code_mapper(source_med_sdf, concept_sdf, oid_to_vocab_sdf, concept_map_sdf)

    # Add the prescriber
    source_med_sdf = source_med_sdf.alias("m").join(ohdsi_provider_sdf.alias("p"),
                                                                F.col("m.k_provider") == F.col(
                                                                    "p.provider_source_value"), how="left_outer"). \
        select("m.*", F.col("p.provider_id").alias("g_provider_id"))

    source_matched_med_sdf = three_level_standard_code_mapper(source_med_sdf, concept_sdf, oid_to_vocab_sdf, concept_map_sdf,
                                                      "m_drug_code", "m_drug_code_type_oid",
                                                      "s_drug_code", "s_drug_code_type_oid",
                                                      "g_drug_code", "g_drug_code_type_oid",
                                                      "g_drug_source_concept_id", "g_drug_concept_id", mapped_domain_id="Drug")

    source_matched_med_sdf = source_matched_med_sdf.withColumn("mapped_domain_id", F.col("g_drug_concept_id_domain_id"))

    source_matched_med_sdf = source_matched_med_sdf.withColumn("g_source_table_name", F.lit("source_medication"))

    source_matched_med_sdf = write_source_to_parquet_partitioned_by_domain(source_matched_med_sdf, output_path,
                                                                               "source_med_matched")

    source_drug_domain_sdf = source_matched_med_sdf.filter((F.col("mapped_domain_id") == F.lit("Drug")) | (F.col("mapped_domain_id").isNull()))

    drug_field_map = {
        "g_id": "drug_exposure_id",
        "g_person_id": "person_id",
        "g_visit_occurrence_id": "visit_occurrence_id",
        "g_drug_concept_id": "drug_concept_id",
        "g_drug_source_concept_id": "drug_source_concept_id",
        "g_drug_code_with_name": "drug_source_value",
        "s_start_medication_datetime": "drug_exposure_start_datetime",
        "s_end_medication_datetime": "drug_exposure_end_datetime",
        "g_start_medication_date": "drug_exposure_start_date",
        "g_end_medication_date": "drug_exposure_end_date",
        "s_route": "route_source_value",
        "s_status": "stop_reason",
        "s_dose": "dose_source_value",
        "s_quantity": "quantity",
        "s_dose_unit": "dose_unit_source_value",
        "g_drug_type_concept_id": "drug_type_concept_id",
        "g_route_concept_id": "route_concept_id",
        "s_id": "s_id",
        "g_provider_id": "provider_id",
        "g_source_table_name": "g_source_table_name"
    }

    drug_source_path = root_source_path + "drug/"

    source_drug_domain_sdf = mapping_utilities.map_table_column_names(source_drug_domain_sdf, drug_field_map)
    ohdsi_drug_sdf = mapping_utilities.column_names_to_align_to(source_drug_domain_sdf, ohdsi.DrugExposureObject())

    ohdsi_drug_sdf, _ = mapping_utilities.write_parquet_file_and_reload(spark, ohdsi_drug_sdf,
                                                                     "drug_exposure",
                                                                     drug_source_path)

    # TODO: Mapped domain device

    drug_exposure_build_end_time = time.time()
    logging.info(f"Finished building main drug_exposure table ({format_log_time(drug_exposure_build_start_time, drug_exposure_build_end_time)})")

    ohdsi_sdf_dict["drug_exposure"] += [ohdsi_drug_sdf]

    if evaluate_samples:
        generate_local_samples(ohdsi_drug_sdf, local_path, "drug_exposure")

    # Measurement (labs and clinical events)
    logging.info("Started processing source_result")
    source_result_process_start_time = time.time()

    source_result_sdf = prepared_source_sdf_dict["source_result"]
    source_result_sdf = filter_out_i_excluded(source_result_sdf)
    source_result_sdf = align_to_visit(source_result_sdf, ohdsi_person_sdf, visit_source_link_sdf)

    source_result_sdf = result_source_code_mapper(source_result_sdf, concept_sdf, oid_to_vocab_sdf)

    source_result_sdf = result_unit_code_mapper(source_result_sdf, concept_sdf, oid_to_vocab_sdf, concept_map_sdf)

    source_result_sdf = result_code_mapper(source_result_sdf, concept_sdf, oid_to_vocab_sdf, concept_map_sdf)

    source_result_sdf = operator_code_mapper(source_result_sdf, concept_sdf, oid_to_vocab_sdf)

    source_result_sdf = source_result_sdf.withColumn("g_obtained_date", F.to_date(F.col("s_obtained_datetime")))

    source_result_sdf = source_result_sdf.withColumn("g_code", F.expr("coalesce(m_code, s_code)"))

    source_result_sdf = source_result_sdf.withColumn("g_result_text", F.expr("coalesce(m_result_text, s_result_text)"))

    source_result_sdf = source_result_sdf.withColumn("g_result_numeric", F.coalesce(F.col("m_result_numeric"), F.col("s_result_numeric")).cast("double"))

    source_result_sdf = source_result_sdf.withColumn("g_result_numeric_lower",
                                                     F.col("s_result_numeric_lower").cast("double"))

    source_result_sdf = source_result_sdf.withColumn("g_result_numeric_upper",
                                                     F.col("s_result_numeric_upper").cast("double"))

    source_result_matched_sdf = mapped_and_source_standard_code_mapper(source_result_sdf, concept_sdf, oid_to_vocab_sdf, concept_map_sdf,
                                           "m_code", "m_code_type_oid", "s_code", "s_code_type_oid",
                                           "g_measurement_source_concept_id", "g_measurement_concept_id")

    source_result_matched_sdf = source_result_matched_sdf.withColumn("mapped_domain_id",
                                                                     F.col("g_measurement_concept_id_domain_id"))

    source_result_matched_sdf = source_result_matched_sdf.withColumn("domain_id",
                                                                     F.col("g_measurement_concept_id_domain_id"))

    source_result_matched_sdf = source_result_matched_sdf.withColumn("g_source_table_name", F.lit("source_result"))

    source_result_matched_sdf = write_source_to_parquet_partitioned_by_domain(source_result_matched_sdf,
                                                                              output_path,
                                                                              "source_result_matched")

    result_field_map = {
        "g_id": "measurement_id",
        "g_person_id": "person_id",
        "g_visit_occurrence_id": "visit_occurrence_id",
        "g_measurement_type_concept_id": "measurement_type_concept_id",
        "g_measurement_concept_id": "measurement_concept_id",
        "g_measurement_source_concept_id": "measurement_source_concept_id",
        "g_code": "measurement_source_value",
        "s_obtained_datetime": "measurement_datetime",
        "g_obtained_date": "measurement_date",
        "g_value_as_concept_id": "value_as_concept_id",
        "g_result_numeric": "value_as_number",
        "g_result_text": "value_source_value",
        "g_unit_concept_id": "unit_concept_id",
        "s_result_unit": "unit_source_value",
        "g_result_numeric_lower": "range_low",
        "g_result_numeric_upper": "range_high",
        "g_operator_concept_id": "operator_concept_id",
        "s_id": "s_id",
        "g_source_table_name": "g_source_table_name"
    }

    result_source_path = root_source_path + "result/"

    ohdsi_measurement_sdf = build_mapped_domain_df(spark, source_result_matched_sdf,
                                                                field_map=result_field_map,
                                                                output_obj=ohdsi.MeasurementObject(),
                                                                domain="Measurement",
                                                                table_name="measurement",
                                                                source_path=result_source_path)

    ohdsi_sdf_dict["measurement"] += [ohdsi_measurement_sdf]

    if evaluate_samples:
        generate_local_samples(ohdsi_measurement_sdf, local_path, "measurement")

    if ohdsi_version == "5.3.1":
        observation_field_map = {
            "g_id": "observation_id",
            "g_person_id": "person_id",
            "g_visit_occurrence_id": "visit_occurrence_id",
            "g_measurement_type_concept_id": "observation_type_concept_id",
            "g_measurement_concept_id": "observation_concept_id",
            "g_measurement_source_concept_id": "observation_source_concept_id",
            "g_code": "observation_source_value",
            "s_obtained_datetime": "observation_datetime",
            "g_obtained_date": "observation_date",
            "g_value_as_concept_id": "value_as_concept_id",
            "g_result_numeric": "value_as_number",
            "g_result_text": "value_as_string",
            "g_unit_concept_id": "unit_concept_id",
            "s_result_unit": "unit_source_value",
            "s_id": "s_id",
            "g_source_table_name": "g_source_table_name"
        }
    elif ohdsi_version == "5.4.1":

        observation_field_map = {
            "g_id": "observation_id",
            "g_person_id": "person_id",
            "g_visit_occurrence_id": "visit_occurrence_id",
            "g_measurement_type_concept_id": "observation_type_concept_id",
            "g_measurement_concept_id": "observation_concept_id",
            "g_measurement_source_concept_id": "observation_source_concept_id",
            "g_code": "observation_source_value",
            "s_obtained_datetime": "observation_datetime",
            "g_obtained_date": "observation_date",
            "g_value_as_concept_id": "value_as_concept_id",
            "g_result_numeric": "value_as_number",
            "g_result_text": "value_as_string",
            "g_unit_concept_id": "unit_concept_id",
            "s_result_unit": "unit_source_value",
            "s_id": "s_id",
            "g_source_table_name": "g_source_table_name"
        }

    ohdsi_observation_sdf = build_mapped_domain_df(spark, source_result_matched_sdf,
                                                   field_map=observation_field_map,
                                                   output_obj=ohdsi.ObservationObject(),
                                                   domain="Observation",
                                                   table_name="observation",
                                                   source_path=result_source_path)

    ohdsi_sdf_dict["observation"] += [ohdsi_observation_sdf]

    source_result_process_end_time = time.time()
    logging.info(f"Finished processing source_result (Total elapsed time: {format_log_time(source_result_process_start_time, source_result_process_end_time)})")

    if evaluate_samples:
        generate_local_samples(ohdsi_observation_sdf, local_path, "observation")

    logging.info("Generating metadata")
    if write_cdm_source:

        cdm_release_date = datetime.datetime.utcnow()

        if ohdsi_version == "5.3.1":
            cdm_version = "CDM v5.3.1"

        elif ohdsi_version == "5.4.1":
            cdm_version = "CDM v5.4.1"
        else:
            cdm_version = None

        if "cdm_source" in config:
            cdm_source_dict = config["cdm_source"]
        else:
            cdm_source_dict = {}

        if "cdm_source_name" in cdm_source_dict:
            cdm_source_name = cdm_source_dict["cdm_source_name"]
        else:
            cdm_source_name = "Prepared source mapped to OHDSI"

        if "cdm_source_abbreviation" in cdm_source_dict:
            cdm_source_version = cdm_source_dict["cdm_source_version"]
        else:
            cdm_source_version = "ps2ohdsi"

        if "cdm_holder" in cdm_source_dict:
            cdm_holder = cdm_source_dict["cdm_holder"]
        else:
            cdm_holder = "Not specified"

        if "source_description" in cdm_source_dict:
            source_description = cdm_source_dict["source_description"]
        else:
            source_description = None

        if "source_description_reference" in cdm_source_dict:
            source_description_reference = cdm_source_dict["source_description_reference"]
        else:
            source_description_reference = None

        if "source_release_date" in cdm_source_dict:
            source_release_date = cdm_source_dict["source_release_date"]
        else:
            source_release_date = cdm_release_date


        cdm_source_dict = {
            "cdm_source_name": cdm_source_name,
            "cdm_source_abbreviation": cdm_source_version,
            "cdm_holder": cdm_holder,
            "source_description": source_description,
            "source_documentation_reference": source_description_reference,
            "cdm_etl_reference": "https://github.com/jhajagos/PreparedSource2OHDSI",
            "source_release_date": source_release_date,
            "cdm_release_date": cdm_release_date,
            "cdm_version": cdm_version,

        }

        if ohdsi_version == "5.4.1":

            vocabulary_sdf = concept_map_sdf_dict["vocabulary"]
            vocabulary_version = vocabulary_sdf.where(F.col("vocabulary_id") == F.lit("None")).select(F.col("vocabulary_version")).toPandas().values[0][0]  #SELECT vocabulary_version from vocabulary  where vocabulary_id = 'None'
            cdm_version_source_df = concept_sdf.where((F.col("vocabulary_id") == F.lit("CDM")) & (F.col("concept_class_id") == F.lit("CDM")) & (F.col("concept_code") == F.lit(cdm_version))).toPandas()
            if len(cdm_version_source_df):
                cdm_version_source_id = cdm_version_source_df.values[0][0]
            else:
                cdm_version_source_id = None

            cdm_source_dict["vocabulary_version"] = vocabulary_version
            cdm_source_dict["cdm_version_concept_id"] = cdm_version_source_id

    logging.info(str(cdm_source_dict))

    empty_tables_dict = {
        "dose_era": ohdsi.DoseEraObject(),
        "drug_era": ohdsi.DrugEraObject(),
        "condition_era": ohdsi.ConditionEraObject(),

        "specimen": ohdsi.SpecimenObject(),

        "note": ohdsi.NoteObject(),
        "note_nlp": ohdsi.NoteNlpObject(),

        "cost": ohdsi.CostObject(),
        "cohort_definition": ohdsi.CohortDefinitionObject(),

        "fact_relationship": ohdsi.FactRelationshipObject(),

        "metadata": ohdsi.MetadataObject(),
        "cdm_source": ohdsi.CdmSourceObject(),
    }

    exported_table_dict["ohdsi"] = {}
    if write_cdm_source:
        empty_tables_dict.pop("cdm_source")

        cdm_source_schema = mapping_utilities.create_schema_from_table_object(ohdsi.CdmSourceObject(), add_g_id=False)

        cdm_source_sdf = spark.createDataFrame([cdm_source_dict], cdm_source_schema)

        cdm_source_sdf, sdf_path = mapping_utilities.write_parquet_file_and_reload(spark, cdm_source_sdf, "cdm_source", output_path)

        exported_table_dict["ohdsi"]["cdm_source"] = sdf_path

    if ohdsi_version == "5.3.1":
        empty_tables_dict["attribute_definition"] = ohdsi.AttributeDefinitionObject()

    elif ohdsi_version == "5.4.1":
        empty_tables_dict["episode"] = ohdsi.EpisodeObject()
        empty_tables_dict["episode_event"] = ohdsi.EpisodeEventObject()


    # Add empty tables
    logging.info(f"Processing empty tables")

    for table in empty_tables_dict:
        sdf = mapping_utilities.create_empty_table_from_table_object(spark, empty_tables_dict[table], add_g_id=False)
        sdf, sdf_path = mapping_utilities.write_parquet_file_and_reload(spark, sdf, table, output_path)
        exported_table_dict["ohdsi"][table] = sdf_path

    # Combine tables that are from multiple source domain
    logging.info("Combining mapped source tables")
    combine_tables_start_time = time.time()
    tables_to_combine = ["procedure_occurrence", "drug_exposure", "measurement", "condition_occurrence", "observation",
                         "device_exposure"]

    logging.info("Combining tables")
    combined_tables_dict = {}
    for table_name in tables_to_combine:
        logging.info(f"Building {table_name}")
        combined_df = union_mapped_tables(ohdsi_sdf_dict[table_name], primary_key_dict[table_name])
        combined_df, combined_path = mapping_utilities.write_parquet_file_and_reload(spark, combined_df, table_name, output_path)
        combined_tables_dict[table_name] = combined_df, combined_path

    combine_tables_end_time = time.time()
    logging.info(f"Finished combining tables (Total elapsed time: {format_log_time(combine_tables_start_time, combine_tables_end_time)})")

    for table_name in tables_to_combine:
        exported_table_dict["ohdsi"][table_name] = combined_tables_dict[table_name][1]

    for table_name in ohdsi_sdf_dict:
        sdf_key = ohdsi_sdf_dict[table_name]
        if type(sdf_key) == list:
            pass
        else:
            exported_table_dict["ohdsi"][table_name] = sdf_key[1]

    with open(export_json_file_name, "w") as fw:
        json.dump(exported_table_dict, fw, sort_keys=True, indent=4, separators=(',', ': '))

    # Generate table counts
    if compute_data_checks:
        logging.info("Row counts for concept tables")
        for key_name in concept_map_sdf_dict:
            logging.info({key_name: concept_map_sdf_dict[key_name].count()})

        logging.info("Row counts for prepared_source tables")
        for key_name in prepared_source_sdf_dict:
            logging.info({key_name: prepared_source_sdf_dict[key_name].count()})

        logging.info("Row counts for OHDSI CDM tables")
        for key_name in ohdsi_sdf_dict:
            if type(ohdsi_sdf_dict[key_name]) == list:
                logging.info({key_name: [sdf.count() for sdf in ohdsi_sdf_dict[key_name]]})
            else:
                logging.info({key_name: ohdsi_sdf_dict[key_name][0].count()})

    ending_time = time.time()
    logging.info(f"Finished mapping Prepared Source Tables to OHDSI CDM (Total script time: {format_log_time(starting_time, ending_time)}))")


def align_to_visit(source_sdf, ohdsi_person_sdf, visit_source_link_sdf):
    source_sdf = filter_out_i_excluded(source_sdf)
    source_sdf = align_to_person_id(source_sdf, ohdsi_person_sdf)

    source_sdf = source_sdf.alias("c").join(visit_source_link_sdf.alias("v"),
                                            (F.col("c.s_encounter_id") == F.col("v.s_encounter_id")) &
                                            (F.col("c.s_person_id") == F.col("v.s_person_id")), how="left_outer")

    source_sdf = source_sdf.select("c.*", F.col("v.visit_occurrence_id").alias("g_visit_occurrence_id"))

    return source_sdf


def map_codes_to_domain(source_sdf, concept_sdf, oid_to_vocab_sdf, concept_map_sdf, code_field_name, code_field_oid,
                        id_field_name, source_table_name, join_type="inner"):
    """For a source table align code to a standardized vocabulary and determine the mapped domain"""

    matched_source_sdf = code_and_oid_to_concept_id_mapper(source_sdf, concept_sdf, oid_to_vocab_sdf,
                                                           code_field_name, code_field_oid)

    matched_source_sdf = map_to_standard_concept_code(matched_source_sdf, concept_map_sdf)
    matched_source_sdf = matched_source_sdf.withColumn(id_field_name, F.monotonically_increasing_id())

    source_matched_sdf = source_sdf.alias("s").join(matched_source_sdf.alias("m"),
                                            F.col("s.g_id") == F.col("m.g_id"),
                                            how=join_type)

    if join_type == "left_outer":
        source_matched_sdf = source_matched_sdf.withColumn("concept_id",
                                                           F.expr("case when concept_id is null then 0 else concept_id end"))
        source_matched_sdf = source_matched_sdf.withColumn("mapped_concept_id",
                                                           F.expr("case when mapped_concept_id is null then 0 else mapped_concept_id end"))

    source_matched_sdf = source_matched_sdf.select("s.*", "code_field_name", "code", "vocabulary_id",
                                           F.col("concept_id").alias("g_source_concept_id"),
                                           "standard_concept", "domain_id",
                                           F.col("mapped_concept_id").alias("g_concept_id"),
                                           "mapped_vocabulary_id", "mapped_domain_id", "relationship_id",
                                           F.lit(source_table_name).alias("g_source_table_name"),
                                            id_field_name)

    source_matched_sdf = source_matched_sdf.distinct()

    return source_matched_sdf


def write_source_to_parquet_partitioned_by_domain(source_matched_sdf, base_path, table_name):
    """Write out partitioned support table to parquet file partitioned by the mapped domain_id"""
    source_matched_path = base_path + "support/" + table_name +".parquet"
    logging.info(f"Writing '{table_name}' to '{source_matched_path}'")

    if source_matched_sdf.count():
        source_matched_sdf.write.mode("overwrite").partitionBy("mapped_domain_id").parquet(source_matched_path)
    else:
        source_matched_sdf.write.mode("overwrite").parquet(source_matched_path)

    source_matched_sdf = spark.read.parquet(source_matched_path)

    return source_matched_sdf


def code_and_oid_to_concept_id_mapper(source_sdf, concept_sdf, oid_vocab_sdf, code_field_name, oid_field_name,
                                      match_level=1, id_field_name="g_id"):
    """Directly maps code with oid to an ohdsi concept_id"""

    logging.info(f"Mapping: '{code_field_name}', '{oid_field_name}'")

    code_join_sdf = source_sdf.alias("s").filter((F.col("s." + code_field_name).isNotNull()) &
                                                 (F.col("s." + oid_field_name).isNotNull())).\
        join(oid_vocab_sdf.alias("o"), F.col("s." + oid_field_name) == F.col("o.oid")).\
        join(concept_sdf.alias("c"), (F.col("c.vocabulary_id") == F.col("o.vocabulary_id")) &
             (F.col("s." + code_field_name) == F.col("c.concept_code")))

    code_join_sdf = code_join_sdf.select(F.lit(match_level).alias("match_level"),
                                         F.col("s." + id_field_name),
                                         F.lit(code_field_name).alias("code_field_name"),
                                         F.col("s." + code_field_name).alias("code"),
                                         F.col("c.vocabulary_id"),
                                         F.col("c.concept_id"),
                                         F.col("c.standard_concept"),
                                         F.col("c.domain_id"))
    return code_join_sdf


def map_to_standard_concept_code(match_sdf, concept_map_sdf):
    """Maps source concept_id to the OHDSI standard concept_id"""

    standard_match_sdf = match_sdf.alias("s").join(concept_map_sdf.alias("cm"),
                                   (F.col("s.concept_id") == F.col("cm.source_concept_id")) &
                                   (F.col("s.vocabulary_id") == F.col("cm.source_vocabulary_id")), how="left_outer")

    standard_match_sdf = standard_match_sdf.select("s.*", F.col("cm.mapped_concept_id"),
                                                   F.col("cm.mapped_vocabulary_id"),
                                                   F.col("cm.mapped_domain_id"),
                                                   F.col("cm.relationship_id"))

    return standard_match_sdf


def select_top_matched_code(matches_sdf):
    """If we have multiple matches from source (s_) and mapped (m_) fields uses match level to find top match"""

    first_matches_sdf = matches_sdf[0]
    rest_of_matches = matches_sdf[1:]

    match_sdf = first_matches_sdf
    for another_match_sdf in rest_of_matches:
        match_sdf = match_sdf.union(another_match_sdf)

    window_g_id = Window.partitionBy("g_id").orderBy("match_level")

    match_sdf = match_sdf.withColumn("match_rank", F.row_number().over(window_g_id))

    best_match_sdf = match_sdf.filter(F.col("match_rank") == F.lit(1))

    return best_match_sdf


def mapped_and_source_standard_code_mapper(source_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf, m_code, m_code_oid, s_code, s_code_oid,
                                           source_concept_id_field_name, concept_id_field_name, mapped_domain_id=None):
    """Inputs two levels of codes to map and selects the best"""
    return three_level_standard_code_mapper(source_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf, m_code, m_code_oid, s_code, s_code_oid,
                                            None, None, source_concept_id_field_name, concept_id_field_name, mapped_domain_id)


def three_level_standard_code_mapper(source_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf, m_code, m_code_oid,
                                     s_code, s_code_oid, g_code, g_code_oid,
                                     source_concept_id_field_name, concept_id_field_name, mapped_domain_id):
    """Inputs three levels of codes to map and selects the best
        mapped_domain_id: sets the mapped concept id to zero if it is not in the specified domain"""

    logging.info(f"m_code={m_code}, m_code_oid={m_code_oid}, s_code={s_code}, s_code_oid={s_code_oid}")
    logging.info(f"source_concept_id_field_name={source_concept_id_field_name}, concept_id_field_name={concept_id_field_name}")
    logging.info(f"Restrict mapped_domain_id={mapped_domain_id}")

    match_list = []
    if m_code is not None:
        match_sdf_1 = code_and_oid_to_concept_id_mapper(source_sdf, concept_sdf, oid_vocab_sdf,
                                                        m_code, m_code_oid, match_level=1)
        match_list += [match_sdf_1]

    if s_code is not None:
        match_sdf_2 = code_and_oid_to_concept_id_mapper(source_sdf, concept_sdf, oid_vocab_sdf,
                                                        s_code, s_code_oid, match_level=2)
        match_list += [match_sdf_2]

    if g_code is not None:
        match_sdf_3 = code_and_oid_to_concept_id_mapper(source_sdf, concept_sdf, oid_vocab_sdf,
                                                        g_code, g_code_oid, match_level=3)

        match_list += [match_sdf_3]

    match_sdf = select_top_matched_code(match_list)

    # Map to standard concept
    standard_match_sdf = map_to_standard_concept_code(match_sdf, concept_map_sdf)

    source_sdf = source_sdf.alias("p").join(standard_match_sdf.alias("m"), F.col("p.g_id") == F.col("m.g_id"),
                                            how="left_outer")

    mapped_domain_field_name = concept_id_field_name + "_domain_id"
    source_sdf = source_sdf.select("p.*", F.col("m.concept_id").alias(source_concept_id_field_name),
                                                 F.col("m.mapped_concept_id").alias(concept_id_field_name),
                                                 F.col("m.mapped_domain_id").alias(mapped_domain_field_name))

    source_sdf = source_sdf.withColumn(source_concept_id_field_name,
                                                     F.expr(f"case when {source_concept_id_field_name} is null then 0 else {source_concept_id_field_name} end"))

    if mapped_domain_id is None:
        source_sdf = source_sdf.withColumn(concept_id_field_name,
                                                     F.expr(f"case when {concept_id_field_name} is null then 0 else {concept_id_field_name} end"))
    else:
        source_sdf = source_sdf.withColumn(concept_id_field_name,
                                           F.expr(
                                               f"case when {concept_id_field_name} is null then 0 when {mapped_domain_field_name} != '{mapped_domain_id}' then 0 else {concept_id_field_name} end"))

    source_sdf = source_sdf.distinct()

    if CHECK_POINTING == "LOCAL":
        source_sdf = source_sdf.localCheckpoint()  # Local checkpoint otherwise DAG (query plan) explode
    elif CHECK_POINTING == "REMOTE":
        source_sdf = source_sdf.checkpoint()
    else:
        pass

    return source_sdf


def source_standard_code_mapper(source_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf, s_code, s_code_oid,
                                           source_concept_id_field_name, concept_id_field_name, mapped_domain_id=None):
    return mapped_and_source_standard_code_mapper(source_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf, s_code, s_code_oid,
                                                  None, None,
                                           source_concept_id_field_name, concept_id_field_name, mapped_domain_id=mapped_domain_id)


def standard_code_mapper(source_sdf, concept_sdf, oid_vocab_sdf, code, code_oid, concept_id_field_name):
    """Maps codes which are standard concept_id"""

    match_sdf = code_and_oid_to_concept_id_mapper(source_sdf, concept_sdf, oid_vocab_sdf,
                                                           code, code_oid, match_level=1)

    match_sdf = match_sdf.filter(F.col("standard_concept") == F.lit("S"))

    source_sdf = source_sdf.alias("p").join(match_sdf.alias("m"), F.col("p.g_id") == F.col("m.g_id"),
                                            how="left_outer")

    source_sdf = source_sdf.select("p.*", F.col("m.concept_id").alias(concept_id_field_name))

    source_sdf = source_sdf.withColumn(concept_id_field_name,
                                       F.expr(
                                           f"case when {concept_id_field_name} is null then 0 else {concept_id_field_name} end"))
    if CHECK_POINTING == "LOCAL":
        source_sdf = source_sdf.localCheckpoint()  # Local checkpoint otherwise DAG (query plan) explodes
    elif CHECK_POINTING == "REMOTE":
        source_sdf = source_sdf.checkpoint()
    else:
        pass

    return source_sdf


# Wrapper functions for mapping specific code types
def gender_code_mapper(source_person_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf):
    return mapped_and_source_standard_code_mapper(source_person_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf,
                                "m_gender_code", "m_gender_code_type_oid",
                                "s_gender_code", "s_gender_code_type_oid",
                                "g_gender_source_concept_id", "g_gender_concept_id", mapped_domain_id="Gender")


def race_code_mapper(source_person_sdf, concept_sdf,  oid_vocab_sdf, concept_map_sdf):
    return mapped_and_source_standard_code_mapper(source_person_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf,
                                "m_race_code", "m_race_code_type_oid",
                                "s_race_code", "s_race_code_type_oid",
                                "g_race_source_concept_id", "g_race_concept_id", mapped_domain_id="Race")


def ethnicity_code_mapper(source_person_sdf, concept_sdf,  oid_vocab_sdf, concept_map_sdf):
    return mapped_and_source_standard_code_mapper(source_person_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf,
                                "m_ethnicity_code", "m_ethnicity_code_type_oid",
                                "s_ethnicity_code", "s_ethnicity_code_type_oid",
                                "g_ethnicity_source_concept_id", "g_ethnicity_concept_id", mapped_domain_id="Ethnicity")


def death_source_code_mapper(source_death_sdf, concept_sdf, oid_vocab_sdf):
    return standard_code_mapper(source_death_sdf, concept_sdf, oid_vocab_sdf,
                                "m_death_source_code", "m_death_source_code_type_oid",
                                "g_death_type_concept_id")


def visit_code_mapper(source_encounter_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf):
    return mapped_and_source_standard_code_mapper(source_encounter_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf,
                                "m_visit_type_code", "m_visit_type_code_type_oid",
                                "s_visit_type_code", "s_visit_type_code_type_oid",
                                "g_visit_source_concept_id", "g_visit_concept_id", mapped_domain_id="Visit")


def visit_source_code_mapper(source_encounter_sdf, concept_sdf, oid_vocab_sdf):
    return standard_code_mapper(source_encounter_sdf, concept_sdf, oid_vocab_sdf,
                                "m_visit_source_code", "m_visit_source_code_type_oid",
                                "g_visit_type_concept_id")


def visit_detail_code_mapper(source_encounter_detail_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf):
    return mapped_and_source_standard_code_mapper(source_encounter_detail_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf,
                                "m_visit_detail_type_code", "m_visit_detail_type_code_type_oid",
                                "s_visit_detail_type_code", "s_visit_detail_type_code_type_oid",
                                "g_visit_detail_source_concept_id", "g_visit_detail_concept_id", mapped_domain_id="Visit")


def visit_detail_source_code_mapper(source_encounter_detail_sdf, concept_sdf, oid_vocab_sdf):
    return standard_code_mapper(source_encounter_detail_sdf, concept_sdf, oid_vocab_sdf,
                                "m_visit_detail_source_code", "m_visit_detail_source_code_type_oid",
                                "g_visit_detail_type_concept_id")


def admit_source_code_mapper(source_encounter_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf):
    return mapped_and_source_standard_code_mapper(source_encounter_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf,
                                "m_admitting_source_code", "m_admitting_source_code_type_oid",
                                "s_admitting_source_code", "s_admitting_source_code_type_oid",
                                "g_admitting_source_source_concept_id", "g_admitting_source_concept_id")


def discharge_to_code_code_mapper(source_encounter_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf):
    return mapped_and_source_standard_code_mapper(source_encounter_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf,
                                "m_discharge_to_code", "m_discharge_to_code_type_oid",
                                "s_discharge_to_code", "s_discharge_to_code_type_oid",
                                "g_discharge_to_source_concept_id", "g_discharge_to_concept_id")


def observation_period_source_code_mapper(source_observation_period_sdf, concept_sdf, oid_vocab_sdf):
    return standard_code_mapper(source_observation_period_sdf, concept_sdf, oid_vocab_sdf,
                                "m_source_code", "m_source_code_type_oid",
                                "g_period_type_concept_id")


def payer_source_code_code_mapper(source_payer_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf):
    return mapped_and_source_standard_code_mapper(source_payer_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf,
                                "m_payer_code", "m_payer_code_type_oid",
                                "s_payer_code", "s_payer_code_type_oid",
                                "g_payer_source_concept_id", "g_payer_concept_id")


def condition_type_code_mapper(source_condition_sdf, concept_sdf, oid_vocab_sdf):
    return standard_code_mapper(source_condition_sdf, concept_sdf, oid_vocab_sdf,
                                "m_condition_type_code", "m_condition_type_code_type_oid",
                                "g_condition_type_concept_id")


def condition_status_code_mapper(source_condition_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf):
    return mapped_and_source_standard_code_mapper(source_condition_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf,
                                "m_condition_status_code", "m_condition_status_code_type_oid",
                                "s_condition_status_code", "s_condition_status_code_type_oid",
                                "g_condition_status_source_concept_id", "g_condition_status_concept_id")


def procedure_type_code_mapper(source_procedure_sdf, concept_sdf, oid_vocab_sdf):
    return standard_code_mapper(source_procedure_sdf, concept_sdf, oid_vocab_sdf,
                                "m_procedure_type_code", "m_procedure_type_code_type_oid",
                                "g_procedure_type_concept_id")


def device_type_code_mapper(source_device_sdf, concept_sdf, oid_vocab_sdf):
    return standard_code_mapper(source_device_sdf, concept_sdf, oid_vocab_sdf,
                                "m_device_type_code", "m_device_type_code_type_oid",
                                "g_device_type_concept_id")


def result_source_code_mapper(source_result_sdf, concept_sdf, oid_vocab_sdf):
    return standard_code_mapper(source_result_sdf, concept_sdf, oid_vocab_sdf,
                                "m_source_code", "m_source_code_type_oid",
                                "g_measurement_type_concept_id")


def result_unit_code_mapper(source_result_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf):
    return mapped_and_source_standard_code_mapper(source_result_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf,
                                "m_result_unit_code", "m_result_unit_code_type_oid",
                                "s_result_unit_code", "s_result_unit_code_type_oid",
                                "g_unit_source_concept_id", "g_unit_concept_id", mapped_domain_id="Unit")


def result_code_mapper(source_result_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf):
    return mapped_and_source_standard_code_mapper(source_result_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf,
                                "m_result_code", "m_result_code_type_oid",
                                "s_result_code", "s_result_code_type_oid",
                                "g_value_as_source_concept_id", "g_value_as_concept_id")


def operator_code_mapper(source_result_sdf, concept_sdf, oid_vocab_sdf):
    return standard_code_mapper(source_result_sdf, concept_sdf, oid_vocab_sdf,
                                "m_operator_code", "m_operator_code_type_oid",
                                "g_operator_concept_id")


def drug_type_code_mapper(source_med_sdf, concept_sdf, oid_vocab_sdf):
    return standard_code_mapper(source_med_sdf, concept_sdf, oid_vocab_sdf,
                                "m_drug_type_code", "m_drug_type_code_type_oid",
                                "g_drug_type_concept_id")


def route_code_mapper(source_med_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf):
    return mapped_and_source_standard_code_mapper(source_med_sdf, concept_sdf, oid_vocab_sdf, concept_map_sdf,
                                "m_route_code", "m_route_code_type_oid",
                                "s_route_code", "s_route_code_type_oid",
                                "g_route_source_concept_id", "g_route_concept_id", mapped_domain_id="Route")


def filter_out_i_excluded(source_sdf):
    """Filter out rows from the mapper"""
    return source_sdf.filter(F.col("i_exclude").isNull() | (F.col("i_exclude") != F.lit(1)))


def align_to_person_id(source_table_sdf, ohdsi_person_sdf):
    source_table_sdf = source_table_sdf.alias("e").join(ohdsi_person_sdf.alias("p"),
                                                                F.col("e.s_person_id") == F.col(
                                                                    "p.person_source_value"), how="inner")

    source_table_sdf = source_table_sdf.select("e.*", F.col("p.person_id").alias("g_person_id"))

    return source_table_sdf


def union_mapped_tables(spark_dataframe_list, identity_column):

    columns = spark_dataframe_list[0].columns
    i = 0
    for sdf in spark_dataframe_list:

        if i == 0:
            union_sdf = sdf
        else:
            union_sdf = union_sdf.union(sdf)
        i += 1

    projected_column_name = "projected_column_" + identity_column
    union_sdf = union_sdf.withColumn(projected_column_name, F.monotonically_increasing_id())

    position_identity_column = columns.index(identity_column)
    columns[position_identity_column] = F.col(projected_column_name).alias(identity_column)

    return union_sdf.select(columns)


def build_domain_filter(domain_name):
    """Build a filter which includes mapped concepts which have standard and non-standard mappings"""
    return ((F.col("mapped_domain_id") == F.lit(domain_name)) | ((F.col("domain_id") == F.lit(domain_name)) &
                                                                 (F.col("mapped_domain_id") == F.lit(0))))


def build_mapped_domain_df(spark_ptr, source_matched_sdf, field_map, output_obj, domain, table_name, source_path):

    domain_source_sdf = source_matched_sdf.filter(build_domain_filter(domain))  # Get only rows matching domain

    domain_source_sdf = mapping_utilities.map_table_column_names(domain_source_sdf, field_map)  # Map column names

    domain_source_sdf = mapping_utilities.column_names_to_align_to(domain_source_sdf, output_obj) # Only include include columns that are in the table

    domain_source_sdf, _ = mapping_utilities.write_parquet_file_and_reload(spark_ptr, domain_source_sdf, table_name, source_path) # Write results out

    return domain_source_sdf


def generate_local_samples(mapped_sdf, local_path, table_name, fraction=0.05, limit_n=10000):
    mapped_sdf.sample(fraction).limit(limit_n).toPandas().to_csv(local_path + table_name + ".csv", index=False)


def format_log_time(start_time, end_time):
    return f"{end_time - start_time} seconds"


def generate_parquet_path(path, table_name):
    return path + table_name + ".parquet"


if __name__ == "__main__":

    arg_parser_obj = argparse.ArgumentParser(description="Convert Prepared Source into an OHDSI format (5.3.1 or 5.4)")

    arg_parser_obj.add_argument("-c", "--config-json", dest="config_json", default="./config_54.json",
                               help="JSON configuration file")

    arg_parser_obj.add_argument("-s", "--evaluate-samples", dest="evaluate_samples", action="store_true",
                               help="Generates local samples in CSV", default=False)

    arg_parser_obj.add_argument("-e", "--compute-checks", dest="compute_checks", action="store_true", default=False)
    arg_parser_obj.add_argument("-l", "--run-local", dest="run_local", default=False, action="store_true")
    arg_parser_obj.add_argument("--spark-config", dest="spark_config_file_name", default=None)

    arg_obj = arg_parser_obj.parse_args()
    RUN_LOCAL = arg_obj.run_local

    with open(arg_obj.config_json, mode="r") as f:
        config_dict = json.load(f)

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
        default_spark_conf_dict["spark.driver.memory"] = "16g"
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
    pprint.pprint(default_spark_conf_dict)

    for key in default_spark_conf_dict:
        sconf.set(key, default_spark_conf_dict[key])

    spark = SparkSession.builder.config(conf=sconf).appName("MapPreparedSourceToOHDSI").getOrCreate()

    export_parquet_json_name = arg_obj.config_json + ".generated.parquet.json"

    print("Configuration:")
    pprint.pprint(config_dict)

    if "check_pointing" in config_dict:
        CHECK_POINTING = config_dict["check_pointing"]

    if "ohdsi_version" in config_dict and config_dict["ohdsi_version"] == "5.4.1":
        ohdsi_version = "5.4.1"
        import preparedsource2ohdsi.ohdsi_cdm_5_4 as ohdsi
    else:
        import preparedsource2ohdsi.ohdsi_cdm_5_3_1 as ohdsi
        ohdsi_version = "5.3.1"

    if ohdsi_version not in ("5.3.1", "5.4.1"):
        raise RuntimeError("Only OHDSI versions 5.3.1 and 5.4 supported")

    main(config_dict, compute_data_checks=arg_obj.compute_checks, evaluate_samples=arg_obj.evaluate_samples,
         export_json_file_name=export_parquet_json_name, ohdsi_version=ohdsi_version)