from pyspark.sql import SparkSession
import argparse
import json


def main(config, generated_tables_dict, schema=None, exclude_concepts=False, exclude_prepared_source_tables=False):

    print(config)
    print(generated_tables_dict)

    jdbc_connection_string = config["jdbc"]["connection_string"]
    jdbc_properties = config["jdbc"]["properties"]

    domains_to_load = ["ohdsi", "concept", "prepared_source"]

    if exclude_concepts:
        domains_to_load = [d for d in domains_to_load if d != "concept"]

    if exclude_prepared_source_tables:
        domains_to_load = [d for d in domains_to_load if d != "prepared_source"]

    for domain in domains_to_load:
        tables_dict = generated_tables_dict[domain]
        for table in tables_dict:
            parquet_path = tables_dict[table]

            print(f"Reading: '{table}'")
            sdf = spark.read.parquet(parquet_path)

            if domain == "prepared_source":  # We don't create transfer tables
                table_name = table.upper()
            else:
                table_name = "transfer" + table.upper()

            if schema is None:
                write_table_name = table_name
            else:
                write_table_name = schema + "." + table_name

            print(f"Writing: '{write_table_name}'")

            sdf.write.jdbc(url=jdbc_connection_string, table=write_table_name, mode="overwrite",
                           properties=jdbc_properties)


if __name__ == "__main__":

    arg_parser_obj = argparse.ArgumentParser(description="Transfer staged OHDSI parquet files to staging tables in a SQL database")

    arg_parser_obj.add_argument("-j", "--json-generated-tables", dest="json_generated_tables", default="../prepared_source/healtheintent/config.json.generated.parquet.json")
    arg_parser_obj.add_argument("-c", "--config-json-file-name", dest="config_json_file_name",  default="../prepared_source/healtheintent/config.json")
    arg_parser_obj.add_argument("-s", "--schema", dest="schema_name", default=None)
    arg_parser_obj.add_argument("-l", "--run-local", dest="run_local", default=False, action="store_true")
    arg_parser_obj.add_argument("-x", "--exclude-concept-tables", dest="exclude_concept_tables", default=False, action="store_true")
    arg_parser_obj.add_argument("--exclude-prepared-source-tables", dest="exclude_prepared_source_tables", default=False, action="store_true")


    arg_obj = arg_parser_obj.parse_args()
    RUN_LOCAL = arg_obj.run_local

    if RUN_LOCAL:
        spark = SparkSession.builder.appName("ToStagingDBTable") \
            .config('spark.driver.extraJavaOptions', '-Duser.timezone=GMT') \
            .config('spark.executor.extraJavaOptions', '-Duser.timezone=GMT') \
            .config('spark.sql.session.timeZone', 'UTC') \
            .config("spark.driver.memory", "16g") \
            .getOrCreate()
    else:
        spark = SparkSession.builder.appName("ToStagingDBTable") \
            .config('spark.driver.extraJavaOptions', '-Duser.timezone=GMT') \
            .config('spark.executor.extraJavaOptions', '-Duser.timezone=GMT') \
            .config('spark.sql.session.timeZone', 'UTC') \
            .getOrCreate()

    with open(arg_obj.json_generated_tables, "r") as f:
        generated_tables = json.load(f)

    with open(arg_obj.config_json_file_name) as f:
        config = json.load(f)

    main(config, generated_tables, schema=arg_obj.schema_name,
         exclude_concepts=arg_obj.exclude_concept_tables,
         exclude_prepared_source_tables=arg_obj.exclude_prepared_source_tables)