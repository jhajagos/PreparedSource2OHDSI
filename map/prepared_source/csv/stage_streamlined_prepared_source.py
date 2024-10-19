from pyspark.sql import SparkSession
import json
import argparse
import logging
from preparedsource2ohdsi import mapping_utilities
from preparedsource2ohdsi import prepared_source

logging.basicConfig(level=logging.INFO)


def main(config, spark):

    prepared_source_map = {
        "source_person": prepared_source.SourcePersonObject(),
        "source_location": prepared_source.SourceLocationObject(),
        "source_care_site": prepared_source.SourceCareSiteObject(),
        "source_encounter": prepared_source.SourceEncounterObject(),
        "source_observation_period": prepared_source.SourceObservationPeriodObject(),
        "source_encounter_detail": prepared_source.SourceEncounterDetailObject(),
        "source_payer": prepared_source.SourcePayerObject(),
        "source_condition": prepared_source.SourceConditionObject(),
        "source_procedure": prepared_source.SourceProcedureObject(),
        "source_medication": prepared_source.SourceMedicationObject(),
        "source_result": prepared_source.SourceResultObject(),
        "source_person_map": prepared_source.SourcePersonMapObject(),
        "source_encounter_map": prepared_source.SourceEncounterMapObject(),
        "source_device": prepared_source.SourceDeviceObject(),
        "source_provider": prepared_source.SourceProviderObject(),
        "source_note": prepared_source.SourceNoteObject()
    }

    if "staging_table_uppercase_file_name" in config:
        staging_table_uppercase_file_name = config["staging_table_uppercase_file_name"]
    else:
        staging_table_uppercase_file_name = False

    prepared_source_tables_to_exclude = config["prepared_source_tables_to_exclude"]
    prepared_source_csv_table_path = config["prepared_source_csv_table_path"]
    staging_base_path = config["prepared_source_table_path"]
    prepared_source_csv_extension = config["prepared_source_csv_extension"]
    staging_table_prefix = config["staging_table_prefix"]

    source_tables = {}
    for table in prepared_source_map:
        if table not in prepared_source_tables_to_exclude:
            if staging_table_uppercase_file_name:
                source_table = staging_table_prefix + table.upper()
            else:
                source_table = staging_table_prefix + table
            source_tables[table] = source_table

    loaded_tables_dict = mapping_utilities.load_csv_to_sparc_df_dict(spark, source_tables, "csv", prepared_source_csv_table_path, prepared_source_csv_extension)

    for table in prepared_source_tables_to_exclude:
        loaded_tables_dict[table] = mapping_utilities.create_empty_table_from_table_object(spark, prepared_source_map[table])

    export_tables_dict = {}
    for table in loaded_tables_dict:
        if table not in prepared_source_tables_to_exclude:
            export_tables_dict[table] = mapping_utilities.column_names_to_align_to(mapping_utilities.distinct_and_add_row_id(loaded_tables_dict[table]), prepared_source_map[table],
                include_unmatched_columns=True)
        else:
            export_tables_dict[table] = mapping_utilities.distinct_and_add_row_id(loaded_tables_dict[table])

    mapping_utilities.export_sdf_dict_to_parquet(export_tables_dict, staging_base_path)


if __name__ == "__main__":

    arg_parser_obj = argparse.ArgumentParser(description="Stage CSV files that confirm to the PreparedSource format for mapping to OHDSI")
    arg_parser_obj.add_argument("-c", "--config-json-file-name", dest="config_json_file_name", default="./config.json")
    arg_parser_obj.add_argument("-l", "--run-local", dest="run_local", default=False, action="store_true")

    arg_obj = arg_parser_obj.parse_args()

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

    main(config, spark)