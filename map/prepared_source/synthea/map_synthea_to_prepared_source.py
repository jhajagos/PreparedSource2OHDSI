from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import argparse
import json
import logging


from preparedsource2ohdsi.mapping_utilities import (load_csv_to_sparc_df_dict, write_parquet_file_and_reload,
                                                    load_local_csv_file, column_names_to_align_to,
                                                    distinct_and_add_row_id, create_empty_table_from_table_object)

import preparedsource2ohdsi.prepared_source as prepared_source

spark = SparkSession.builder.appName("SyntheaToPreparedSource")\
    .config('spark.driver.extraJavaOptions', '-Duser.timezone=GMT') \
    .config('spark.executor.extraJavaOptions', '-Duser.timezone=GMT') \
    .config('spark.sql.session.timeZone', 'UTC') \
    .getOrCreate()

logging.basicConfig(level=logging.INFO)


def main(config):
    # Tables which are not used are commented out

    table_names = [
                    #"allergies",
                    #"careplans",
                    "conditions",
                    "devices",
                    "encounters",
                    #"imaging_studies",
                    "immunizations",
                    "medications",
                    "observations",
                    "organizations",
                    "patients",
                    "payers",
                    "payer_transitions",
                    "procedures",
                    "providers",                    #"supplies"
    ]

    table_dict = {tn: tn for tn in table_names}

    # Load Synthea Tables
    synthea_sdf_dict = load_csv_to_sparc_df_dict(spark, table_dict, file_type=config["file_type"],
                                                 base_path=config["base_path"], extension=config["csv_extension"])

    # Attach Synthea Tables
    for table_name in synthea_sdf_dict:
        synthea_sdf_dict[table_name].createOrReplaceTempView(table_name)

    prepared_source_dict = {}

    sql_source_location = """
    select distinct
    sha1(coalesce(`ADDRESS`, '') || coalesce(`CITY`,'') || coalesce(`STATE`,'') || coalesce(`ZIP`, '')) as k_location
   ,`ADDRESS` as s_address_1
   ,cast(NULL as STRING) as s_address_2
   ,`CITY` as s_city
   ,`STATE` as s_state
   ,`ZIP` as s_zip
   ,`COUNTY` as s_county
   ,'U.S.A.' as s_country
   ,cast(NULL as STRING) as s_location_name
   ,`LAT` as s_latitude
   ,`LON` as s_longitude
from patients p
    """

    source_location_sdf = distinct_and_add_row_id(spark.sql(sql_source_location))

    prepared_source_dict["source_location"], _ = write_parquet_file_and_reload(spark, source_location_sdf, "source_location",
                                                                             config["prepared_source_output_location"])

    race_mapping_sdf = load_local_csv_file(spark, "./mappings/race.csv", table_name="race_mapping")
    ethnicity_mapping_sdf = load_local_csv_file(spark, "./mappings/ethnicity.csv", table_name="ethnicity_mapping")
    gender_mapping_sdf = load_local_csv_file(spark, "./mappings/gender.csv", table_name="gender_mapping")

    sql_source_person = """
    select
    p.`Id` as s_person_id --Source identifier for patient or person
   ,p.`GENDER` as s_gender --Source gender description
   ,cast(NULL as STRING) as s_gender_code --Source gender code
   ,cast(NULL as STRING) as s_gender_code_type --Source gender code type (human readable & OPTIONAL)
   ,cast(NULL as STRING) as s_gender_code_type_oid --Source gender code type specified by OID (used in OHDSI mapping)
   ,gm.m_gender --Mapped by ETL gender description
   ,gm.m_gender_code --Mapped by ETL gender code (See: https://phinvads.cdc.gov/vads/ViewCodeSystem.action?id=2.16.840.1.113883.12.1)
   ,gm.m_gender_code_type --Mapped by ETL gender code type
   ,gm.m_gender_code_type_oid --Mapped by ETL gender code type by OID (Gender 2.16.840.1.113883.12.1)
   ,p.`BIRTHDATE` as s_birth_datetime
   ,p.`DEATHDATE` as s_death_datetime
   ,cast(NULL as STRING) as m_death_source
   ,cast(NULL as STRING) as m_death_source_code
   ,cast(NULL as STRING) as m_death_source_code_type
   ,cast(NULL as STRING) as m_death_source_code_type_oid
   ,p.`RACE` as s_race --Source race (Black, white, etc) description
   ,cast(NULL as STRING) as s_race_code --Source race code
   ,cast(NULL as STRING) as s_race_code_type --Source race code type (human readable & OPTIONAL)
   ,cast(NULL as STRING) as s_race_code_type_oid
   ,rm.m_race
   ,rm.m_race_code --See: https://athena.ohdsi.org/search-terms/terms?domain=Race&standardConcept=Standard&page=1&pageSize=15&query=
   ,rm.m_race_code_type
   ,rm.m_race_code_type_oid --Use 'ohdsi.race' for OHDSI standard race codes
   ,p.`ETHNICITY` as s_ethnicity --Source ethnicity description (Hispanic, Not Hispanic)
   ,cast(NULL as STRING) as s_ethnicity_code --Source ethnicity code
   ,cast(NULL as STRING) as s_ethnicity_code_type --Source gender code type (human readable & OPTIONAL)
   ,cast(NULL as STRING) as s_ethnicity_code_type_oid --Source ethnicity code type specified by OID (used in OHDSI mapping)
   ,em.m_ethnicity
   ,em.m_ethnicity_code --See: https://athena.ohdsi.org/search-terms/terms?domain=Ethnicity&standardConcept=Standard&page=1&pageSize=15&query=
   ,em.m_ethnicity_code_type
   ,em.m_ethnicity_code_type_oid --Use 'ohdsi.ethnicity' for standard OHDSI ethnicity codes
   ,sha1(coalesce(`ADDRESS`, '') || coalesce(`CITY`,'') || coalesce(`STATE`,'') || coalesce(`ZIP`, '')) as k_location --Key value to location in source_location table
   ,cast(NULL as STRING) as i_exclude --Value of 1 instructs the mapper to skip the row
   ,cast(NULL as STRING) as s_id --Row source identifier
   ,'synthea' as s_source_system --Source system row was extracted from
   ,cast(NULL as STRING) as m_source_system --Mapped source system the row was extracted from
from patients p
    left outer join race_mapping rm on p.`RACE` = rm.s_race
    left outer join ethnicity_mapping em on p.`ETHNICITY` = em.s_ethnicity
    left outer join gender_mapping gm on p.`GENDER` = gm.s_gender
    """

    source_person_sdf = distinct_and_add_row_id(spark.sql(sql_source_person))
    prepared_source_dict["source_person"], _ = write_parquet_file_and_reload(spark, source_person_sdf, "source_person", config["prepared_source_output_location"])

    source_care_site_sql = """select distinct 
    `Id` as k_care_site
   ,`NAME` as s_care_site_name from organizations"""

    source_care_site_sdf = distinct_and_add_row_id(spark.sql(source_care_site_sql))
    prepared_source_dict["source_care_site"], _ = write_parquet_file_and_reload(spark, source_care_site_sdf, "source_care_site",
                                                                                config["prepared_source_output_location"])

    source_provider_sql = """
    select
    `Id` as k_provider
   ,`NAME` as s_provider_name
   ,cast(NULL as STRING) as s_npi
from providers
    """

    source_provider_sdf = distinct_and_add_row_id(spark.sql(source_provider_sql))

    prepared_source_dict["source_providers"], _ = write_parquet_file_and_reload(spark, source_provider_sdf,
                                                                                "source_provider",
                                                                                config[
                                                                                    "prepared_source_output_location"])

    visit_type_mapping_sdf = load_local_csv_file(spark, "./mappings/visit_type.csv", table_name="visit_type_mapping")

    source_encounter_sql = """
    select
    `Id` as s_encounter_id --Source identifier for encounter or visit
   ,`PATIENT` as s_person_id --Source identifier for patient or person
   ,`START` as s_visit_start_datetime
   ,`STOP` as s_visit_end_datetime
   ,`ENCOUNTERCLASS` as s_visit_type
   ,cast(NULL as STRING) as s_visit_type_code
   ,cast(NULL as STRING) as s_visit_type_code_type
   ,cast(NULL as STRING) as s_visit_type_code_type_oid
   ,vtm.m_visit_type
   ,vtm.m_visit_type_code
   ,vtm.m_visit_type_code_type
   ,vtm.m_visit_type_code_type_oid
   ,'EHR Encounter Record' as m_visit_source
   ,'OMOP4976900' as m_visit_source_code
   ,'Type' as m_visit_source_code_type
   ,'ohdsi.type_concept' as m_visit_source_code_type_oid
   ,cast(NULL as STRING) as s_discharge_to
   ,cast(NULL as STRING) as s_discharge_to_code
   ,cast(NULL as STRING) as s_discharge_to_code_type
   ,cast(NULL as STRING) as s_discharge_to_code_type_oid
   ,cast(NULL as STRING) as m_discharge_to
   ,cast(NULL as STRING) as m_discharge_to_code
   ,cast(NULL as STRING) as m_discharge_to_code_type
   ,cast(NULL as STRING) as m_discharge_to_code_type_oid
   ,cast(NULL as STRING) as s_admitting_source
   ,cast(NULL as STRING) as s_admitting_source_code
   ,cast(NULL as STRING) as s_admitting_source_code_type
   ,cast(NULL as STRING) as s_admitting_source_code_type_oid
   ,cast(NULL as STRING) as m_admitting_source
   ,cast(NULL as STRING) as m_admitting_source_code
   ,cast(NULL as STRING) as m_admitting_source_code_type
   ,cast(NULL as STRING) as m_admitting_source_code_type_oid
   ,`ORGANIZATION` as k_care_site
   ,`PROVIDER` as k_provider
   ,cast(NULL as STRING) as i_exclude --Value of 1 instructs the mapper to skip the row
   ,`Id` as s_id --Row source identifier
   ,'synthea' as s_source_system --Source system row was extracted from
   ,cast(NULL as STRING) as m_source_system --Mapped source system the row was extracted from
from encounters e
left outer join visit_type_mapping vtm on vtm.s_visit_type = e.`ENCOUNTERCLASS`
    """

    source_encounter_sdf = distinct_and_add_row_id(spark.sql(source_encounter_sql))

    prepared_source_dict["source_encounter"], _ = write_parquet_file_and_reload(spark, source_encounter_sdf,
                                                                               "source_encounter",
                                                                               config["prepared_source_output_location"])

    source_encounter_sdf.createOrReplaceTempView("source_encounter")
    # Observation period
    source_observation_period_sdf = spark.sql("""
            select s_person_id, min(s_visit_start_datetime) as s_start_observation_datetime
            , max(coalesce(s_visit_end_datetime, s_visit_start_datetime)) as s_end_observation_datetime
            from source_encounter where i_exclude is null or i_exclude = 0 group by s_person_id
        """)

    source_observation_period_sdf = column_names_to_align_to(source_observation_period_sdf,
                                                                               prepared_source.SourceObservationPeriodObject())

    source_observation_period_sdf = source_observation_period_sdf.withColumn("m_source", F.lit("EHR")). \
        withColumn("m_source_code", F.lit("OMOP4976890")). \
        withColumn("m_source_code_type", F.lit("OHDSI")). \
        withColumn("m_source_code_type_oid", F.lit("ohdsi.type_concept"))

    source_observation_period_sdf = distinct_and_add_row_id(source_observation_period_sdf)

    prepared_source_dict["source_observation_period"], _ = write_parquet_file_and_reload(spark, source_observation_period_sdf,
                                                                               "source_observation_period",
                                                                               config["prepared_source_output_location"])

    payer_map_sdf = load_local_csv_file(spark, "./mappings/payer.csv", "payer_map")

    source_payer_sql = """
    select
    `PATIENT` as s_person_id --Source identifier for patient or person
   ,cast(`START_YEAR` || '-01-01' as timestamp) as s_payer_start_datetime --The date the plan's coverage started
   ,cast(`END_YEAR` || '-12-31' as timestamp) as s_payer_end_datetime --The date the plan's contribution ended
   ,`NAME` as s_payer --The name of the payer
   ,cast(NULL as STRING) as s_payer_code
   ,cast(NULL as STRING) as s_payer_code_type
   ,cast(NULL as STRING) as s_payer_code_type_oid
   ,pm.m_payer
   ,pm.m_payer_code
   ,pm.m_payer_code_type
   ,pm.m_payer_code_type_oid
   ,cast(NULL as STRING) as s_plan --The plan type e.g, PPO, silver, bronze
   ,cast(NULL as STRING) as s_plan_code
   ,cast(NULL as STRING) as s_plan_code_type
   ,cast(NULL as STRING) as s_plan_code_type_oid
   ,cast(NULL as STRING) as m_plan
   ,cast(NULL as STRING) as m_plan_code
   ,cast(NULL as STRING) as m_plan_code_type
   ,cast(NULL as STRING) as m_plan_code_type_oid
   ,cast(NULL as STRING) as s_contributor
from payer_transitions pt 
    join payers p on pt.`PAYER` = p.`Id`
    join payer_map pm on pm.s_payer = p.`NAME`
    """

    source_payer_sdf = distinct_and_add_row_id(spark.sql(source_payer_sql))

    prepared_source_dict["source_payer"], _ = write_parquet_file_and_reload(spark, source_payer_sdf,
                                                                                "source_payer",
                                                                                config[
                                                                                    "prepared_source_output_location"])

    source_condition_sql_1 = """
    select
    `PATIENT` as s_person_id --Source identifier for patient or person
   ,`ENCOUNTER` as s_encounter_id --Source identifier for encounter or visit
   ,`START` as s_start_condition_datetime
   ,`STOP` as s_end_condition_datetime
   ,`CODE` as s_condition_code
   ,'SNOMED' as s_condition_code_type
   ,'2.16.840.1.113883.6.96' as s_condition_code_type_oid
   ,'EHR' m_condition_type
   ,'OMOP4976890' as m_condition_type_code
   ,'Type' as m_condition_type_code_type
   ,'ohdsi.type_concept' as m_condition_type_code_type_oid
   ,cast(NULL as STRING) as s_condition_status
   ,cast(NULL as STRING) as s_condition_status_code
   ,cast(NULL as STRING) as s_condition_status_code_type
   ,cast(NULL as STRING) as s_condition_status_code_type_oid
   ,cast(NULL as STRING) as m_condition_status
   ,cast(NULL as STRING) as m_condition_status_code
   ,cast(NULL as STRING) as m_condition_status_code_type
   ,cast(NULL as STRING) as m_condition_status_code_type_oid
   ,cast(NULL as STRING) as s_present_on_admission_indicator
   ,cast(NULL as STRING) as s_sequence_id
   ,cast(NULL as STRING) as k_provider
   ,cast(NULL as STRING) as i_exclude --Value of 1 instructs the mapper to skip the row
   ,cast(NULL as STRING) as s_id --Row source identifier
   ,'synthea' as s_source_system --Source system row was extracted from
   ,cast(NULL as STRING) as m_source_system --Mapped source system the row was extracted from
from conditions
    """

    source_condition_sql_2 = """
    select
    `PATIENT` as s_person_id --Source identifier for patient or person
   ,`Id` as s_encounter_id --Source identifier for encounter or visit
   ,`START` as s_start_condition_datetime
   ,`STOP` as s_end_condition_datetime
   ,`REASONCODE` as s_condition_code
   ,'SNOMED' as s_condition_code_type
   ,'2.16.840.1.113883.6.96' as s_condition_code_type_oid
   ,'EHR' m_condition_type
   ,'OMOP4976890' as m_condition_type_code
   ,'Type' as m_condition_type_code_type
   ,'ohdsi.type_concept' as m_condition_type_code_type_oid
   ,'Reason' as s_condition_status
   ,cast(NULL as STRING) as s_condition_status_code
   ,cast(NULL as STRING) as s_condition_status_code_type
   ,cast(NULL as STRING) as s_condition_status_code_type_oid
   ,'Primary diagnosis' as m_condition_status
   ,'OMOP4976972' as m_condition_status_code
   ,'Condition status' as m_condition_status_code_type
   ,'ohdsi.condition_status' as m_condition_status_code_type_oid
   ,cast(NULL as STRING) as s_present_on_admission_indicator
   ,cast(NULL as STRING) as s_sequence_id
   ,cast(NULL as STRING) as k_provider
   ,cast(NULL as STRING) as i_exclude --Value of 1 instructs the mapper to skip the row
   ,cast(NULL as STRING) as s_id --Row source identifier
   ,'synthea' as s_source_system --Source system row was extracted from
   ,cast(NULL as STRING) as m_source_system --Mapped source system the row was extracted from
from encounters where REASONCODE is not NULL
    
    """

    source_condition_sql = source_condition_sql_1 + "\nunion\n" + source_condition_sql_2

    source_condition_sdf = distinct_and_add_row_id(spark.sql(source_condition_sql))

    prepared_source_dict["source_condition"], _ = write_parquet_file_and_reload(spark, source_condition_sdf,
                                                                                "source_condition",
                                                                                config[
                                                                                    "prepared_source_output_location"])

    source_procedure_sql = """
    select
    `PATIENT` as s_person_id --Source identifier for patient or person
   ,`ENCOUNTER` as s_encounter_id --Source identifier for encounter or visit
   ,`DATE` as s_start_procedure_datetime
   ,`DATE` as s_end_procedure_datetime
   ,`CODE` as s_procedure_code
   ,'SNOMED' as s_procedure_code_type
   ,'2.16.840.1.113883.6.96' as s_procedure_code_type_oid
   , 'EHR' as m_procedure_type
   ,'OMOP4976890' as m_procedure_type_code
   ,'Type' as m_procedure_type_code_type
   ,'ohdsi.type_concept' as m_procedure_type_code_type_oid
   ,cast(NULL as STRING) as s_sequence_id
   ,cast(NULL as STRING) as s_modifier
   ,cast(NULL as STRING) as s_modifier_code
   ,cast(NULL as STRING) as s_modifier_code_type
   ,cast(NULL as STRING) as s_modifier_code_type_oid
   ,cast(NULL as STRING) as s_quantity
   ,cast(NULL as STRING) as k_provider
   ,cast(NULL as STRING) as i_exclude --Value of 1 instructs the mapper to skip the row
   ,cast(NULL as STRING) as s_id --Row source identifier
   ,'synthea' as s_source_system --Source system row was extracted from
   ,cast(NULL as STRING) as m_source_system --Mapped source system the row was extracted from
from procedures
    """

    source_procedure_sdf = distinct_and_add_row_id(spark.sql(source_procedure_sql))

    prepared_source_dict["source_procedure"], _ = write_parquet_file_and_reload(spark, source_procedure_sdf,
                                                                                "source_procedure",
                                                                                config[
                                                                                    "prepared_source_output_location"])
    source_device_sql = """
    select
    `PATIENT` as s_person_id --Source identifier for patient or person
   ,`ENCOUNTER` as s_encounter_id --Source identifier for encounter or visit
   ,`START` as s_start_device_datetime
   ,`STOP` as s_end_device_datetime
   ,`DESCRIPTION` as s_device
   ,`CODE` as s_device_code
   ,'SNOMED' as s_device_code_type
   ,'2.16.840.1.113883.6.96' as s_device_code_type_oid
   ,cast(NULL as STRING) as m_device
   ,cast(NULL as STRING) as m_device_code
   ,cast(NULL as STRING) as m_device_code_type
   ,cast(NULL as STRING) as m_device_code_type_oid
   ,`UDI` as s_unique_device_identifier --A unique identifier for the device exposed to
   ,'EHR' as m_device_type
   ,'OMOP4976890' as m_device_type_code
   ,'Type' as m_device_type_code_type
   ,'ohdsi.type_concept' as m_device_type_code_type_oid
   ,'synthea' as s_source_system
   ,cast(NULL as STRING) as m_source_system
   ,cast(NULL as STRING) as i_exclude
   ,cast(NULL as STRING) as s_id
from devices
    """

    source_device_sdf = distinct_and_add_row_id(spark.sql(source_device_sql))

    prepared_source_dict["source_device"], _ = write_parquet_file_and_reload(spark, source_device_sdf,
                                                                                "source_device",
                                                                                config[
                                                                                    "prepared_source_output_location"])

    source_medication_sql_1 = """
    select
    `PATIENT` as s_person_id --Source identifier for patient or person
   ,`ENCOUNTER` as s_encounter_id --Source identifier for encounter or visit
   ,`CODE` as s_drug_code
   ,'RxNorm' as s_drug_code_type
   ,'2.16.840.1.113883.6.88' as s_drug_code_type_oid
   ,cast(NULL as STRING) as m_drug_code
   ,cast(NULL as STRING) as m_drug_code_type
   ,cast(NULL as STRING) as m_drug_code_type_oid
   ,`DESCRIPTION` as s_drug_text
   ,cast(NULL as STRING) as m_drug_text
   ,cast(NULL as STRING) as g_drug_text
   ,cast(NULL as STRING) as g_drug_code
   ,cast(NULL as STRING) as g_drug_code_type
   ,cast(NULL as STRING) as g_drug_code_type_oid
   ,cast(NULL as STRING) as s_drug_alternative_text
   ,`START` as s_start_medication_datetime
   ,`STOP` as s_end_medication_datetime
   ,cast(NULL as STRING) as s_route
   ,cast(NULL as STRING) as s_route_code
   ,cast(NULL as STRING) as s_route_code_type
   ,cast(NULL as STRING) as s_route_code_type_oid
   ,cast(NULL as STRING) as m_route
   ,cast(NULL as STRING) as m_route_code
   ,cast(NULL as STRING) as m_route_code_type
   ,cast(NULL as STRING) as m_route_code_type_oid
   ,`DISPENSES` as s_quantity
   ,cast(NULL as STRING) as s_dose
   ,cast(NULL as STRING) as m_dose
   ,cast(NULL as STRING) as s_dose_unit
   ,cast(NULL as STRING) as s_dose_unit_code
   ,cast(NULL as STRING) as s_dose_unit_code_type
   ,cast(NULL as STRING) as s_dose_unit_code_type_oid
   ,cast(NULL as STRING) as m_dose_unit
   ,cast(NULL as STRING) as m_dose_unit_code
   ,cast(NULL as STRING) as m_dose_unit_code_type
   ,cast(NULL as STRING) as m_dose_unit_code_type_oid
   ,cast(NULL as STRING) as s_status
   ,cast(NULL as STRING) as s_drug_type
   ,cast(NULL as STRING) as s_drug_type_code
   ,cast(NULL as STRING) as s_drug_type_code_type
   ,cast(NULL as STRING) as s_drug_type_code_type_oid
   ,'EHR' as m_drug_type
   ,'OMOP4976890' as m_drug_type_code
   ,'Type' as m_drug_type_code_type
   ,'ohdsi.type_concept' as m_drug_type_code_type_oid
   ,cast(NULL as STRING) as k_provider
   ,cast(NULL as STRING) as i_exclude --Value of 1 instructs the mapper to skip the row
   ,cast(NULL as STRING) as s_id --Row source identifier
   ,'synthea' as s_source_system --Source system row was extracted from
   ,cast(NULL as STRING) as m_source_system --Mapped source system the row was extracted from
from medications
    """

    source_medication_sql_2 = """
        select
        `PATIENT` as s_person_id --Source identifier for patient or person
       ,`ENCOUNTER` as s_encounter_id --Source identifier for encounter or visit
       ,`CODE` as s_drug_code
       ,'CVX' as s_drug_code_type
       ,'2.16.840.1.113883.12.292' as s_drug_code_type_oid
       ,cast(NULL as STRING) as m_drug_code
       ,cast(NULL as STRING) as m_drug_code_type
       ,cast(NULL as STRING) as m_drug_code_type_oid
       ,`DESCRIPTION` as s_drug_text
       ,cast(NULL as STRING) as m_drug_text
       ,cast(NULL as STRING) as g_drug_text
       ,cast(NULL as STRING) as g_drug_code
       ,cast(NULL as STRING) as g_drug_code_type
       ,cast(NULL as STRING) as g_drug_code_type_oid
       ,cast(NULL as STRING) as s_drug_alternative_text
       ,`DATE` as s_start_medication_datetime
       ,cast(NULL as STRING) as s_end_medication_datetime
       ,cast(NULL as STRING) as s_route
       ,cast(NULL as STRING) as s_route_code
       ,cast(NULL as STRING) as s_route_code_type
       ,cast(NULL as STRING) as s_route_code_type_oid
       ,cast(NULL as STRING) as m_route
       ,cast(NULL as STRING) as m_route_code
       ,cast(NULL as STRING) as m_route_code_type
       ,cast(NULL as STRING) as m_route_code_type_oid
       ,cast(NULL as STRING) as s_quantity
       ,cast(NULL as STRING) as s_dose
       ,cast(NULL as STRING) as m_dose
       ,cast(NULL as STRING) as s_dose_unit
       ,cast(NULL as STRING) as s_dose_unit_code
       ,cast(NULL as STRING) as s_dose_unit_code_type
       ,cast(NULL as STRING) as s_dose_unit_code_type_oid
       ,cast(NULL as STRING) as m_dose_unit
       ,cast(NULL as STRING) as m_dose_unit_code
       ,cast(NULL as STRING) as m_dose_unit_code_type
       ,cast(NULL as STRING) as m_dose_unit_code_type_oid
       ,cast(NULL as STRING) as s_status
       ,cast(NULL as STRING) as s_drug_type
       ,cast(NULL as STRING) as s_drug_type_code
       ,cast(NULL as STRING) as s_drug_type_code_type
       ,cast(NULL as STRING) as s_drug_type_code_type_oid
       ,'EHR' as m_drug_type
       ,'OMOP4976890' as m_drug_type_code
       ,'Type' as m_drug_type_code_type
       ,'ohdsi.type_concept' as m_drug_type_code_type_oid
       ,cast(NULL as STRING) as k_provider
       ,cast(NULL as STRING) as i_exclude --Value of 1 instructs the mapper to skip the row
       ,cast(NULL as STRING) as s_id --Row source identifier
       ,'synthea' as s_source_system --Source system row was extracted from
       ,cast(NULL as STRING) as m_source_system --Mapped source system the row was extracted from
    from immunizations"""

    source_medication_sdf = distinct_and_add_row_id(spark.sql(source_medication_sql_1 + "\nunion\n" + source_medication_sql_2))


    prepared_source_dict["source_medication"], _ = write_parquet_file_and_reload(spark, source_medication_sdf,
                                                                                "source_medication",
                                                                                config[
                                                                                    "prepared_source_output_location"])

    result_code_mapping_sdf = load_local_csv_file(spark, "./mappings/result_code.csv", table_name="result_code_mapping")

    source_result_sql = """
    select
    `PATIENT` as s_person_id --Source identifier for patient or person
   ,`ENCOUNTER` as s_encounter_id --Source identifier for encounter or visit
   ,`DATE` as s_obtained_datetime
   ,`DESCRIPTION` as s_name
   ,`CODE` as s_code
   ,'LOINC' as s_code_type
   ,'2.16.840.1.113883.6.1' as s_code_type_oid
   ,cast(NULL as STRING) as m_code
   ,cast(NULL as STRING) as m_code_type
   ,cast(NULL as STRING) as m_code_type_oid
   ,case when `TYPE` = 'text' then `VALUE` else NULL end as s_result_text
   ,cast(NULL as STRING) as m_result_text
   ,case when `TYPE` = 'numeric' then `VALUE` end as s_result_numeric
   ,cast(NULL as STRING) as m_result_numeric
   ,cast(NULL as STRING) as s_result_datetime
   ,cast(NULL as STRING) as s_result_code
   ,cast(NULL as STRING) as s_result_code_type
   ,cast(NULL as STRING) as s_result_code_type_oid
   ,rcm.m_result_code
   ,rcm.m_result_code_type
   ,rcm.m_result_code_type_oid
   ,`UNITS` as s_result_unit
   ,`UNITS` as s_result_unit_code
   ,'UCUM' as s_result_unit_code_type
   ,'2.16.840.1.113883.4.642.3.912' as s_result_unit_code_type_oid
   ,cast(NULL as STRING) as s_result_numeric_lower
   ,cast(NULL as STRING) as s_result_numeric_upper
   ,cast(NULL as STRING) as s_operator
   ,cast(NULL as STRING) as m_operator
   ,cast(NULL as STRING) as m_operator_code
   ,cast(NULL as STRING) as m_operator_code_type
   ,cast(NULL as STRING) as m_operator_code_type_oid
   ,'EHR' as s_source
   ,'OMOP4976890' as m_source_code
   ,'Type' as m_source_code_type
   ,'ohdsi.type_concept' as m_source_code_type_oid
   ,cast(NULL as STRING) as i_exclude --Value of 1 instructs the mapper to skip the row
   ,cast(NULL as STRING) as s_id --Row source identifier
   ,'sythea' as s_source_system --Source system row was extracted from
   ,cast(NULL as STRING) as m_source_system --Mapped source system the row was extracted from
from observations o join result_code_mapping rcm on o.`VALUE` = rcm.s_result_text and `TYPE` = 'text'
    """

    source_result_sdf = distinct_and_add_row_id(spark.sql(source_result_sql))

    prepared_source_dict["source_result"], _ = write_parquet_file_and_reload(spark, source_result_sdf,
                                                                                "source_result",
                                                                                config[
                                                                                    "prepared_source_output_location"])

    # Add empty source_encounter_detail
    source_encounter_detail_sdf = create_empty_table_from_table_object(spark, prepared_source.SourceEncounterDetailObject())
    prepared_source_dict["source_encounter_detail"], _ = write_parquet_file_and_reload(spark, source_encounter_detail_sdf,
                                                                                "source_encounter_detail",
                                                                                config[
                                                                                    "prepared_source_output_location"])

    # Add empty source_person_map
    source_person_map_sdf = create_empty_table_from_table_object(spark, prepared_source.SourcePersonMapObject())
    prepared_source_dict["source_person_map"], _ = write_parquet_file_and_reload(spark, source_person_map_sdf,
                                                                                "source_person_map",
                                                                                config[
                                                                                    "prepared_source_output_location"])


    # Add empty source_encounter_map
    source_encounter_map_sdf = create_empty_table_from_table_object(spark, prepared_source.SourceEncounterMapObject())

    prepared_source_dict["source_encounter_map"], _ = write_parquet_file_and_reload(spark, source_encounter_map_sdf,
                                                                                "source_encounter_map",
                                                                                config[
                                                                                    "prepared_source_output_location"])


if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser(description="Convert Synthea CSV files to Prepared Source Format (PSF)")
    arg_parse_obj.add_argument("-c", "--config-json", dest="config_json", default="./config.json",
                               help="JSON configuration file")

    arg_obj = arg_parse_obj.parse_args()

    with open(arg_obj.config_json) as f:
        config = json.load(f)

    main(config)