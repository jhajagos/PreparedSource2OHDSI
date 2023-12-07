from pyspark.sql import SparkSession
import argparse
import json


def main(config, generated_tables_dict):

    print(config)
    print(generated_tables_dict)

    csv_base_path = config["csv_base_path"]
    domains_to_write = ["concept", "ohdsi"]

    for domain in domains_to_write:
        tables_dict = generated_tables_dict[domain]
        for table in tables_dict:
            parquet_path = tables_dict[table]

            print(f"Reading: '{table}'")
            sdf = spark.read.parquet(parquet_path)

            write_table_name = table + ".csv"

            write_table_path = csv_base_path + write_table_name

            print(f"Writing: '{write_table_path}'")

            sdf.coalesce(1).write.option("header", True).csv(write_table_path)


if __name__ == "__main__":

    arg_parser_obj = argparse.ArgumentParser(description="For small datasets write a single CSV file for Parquet")

    arg_parser_obj.add_argument("-j", "--json-generated-tables", dest="json_generated_tables", default="../prepared_source/healtheintent/config.json.generated.parquet.json")
    arg_parser_obj.add_argument("-c", "--config-json-file-name", dest="config_json_file_name",  default="../prepared_source/healtheintent/config.json")
    arg_parser_obj.add_argument("-l", "--run-local", dest="run_local", default=False, action="store_true")

    arg_obj = arg_parser_obj.parse_args()

    RUN_LOCAL = arg_obj.run_local

    if RUN_LOCAL:
        spark = SparkSession.builder.appName("PreparedSourceToOHDSI") \
            .config('spark.driver.extraJavaOptions', '-Duser.timezone=GMT') \
            .config('spark.executor.extraJavaOptions', '-Duser.timezone=GMT') \
            .config('spark.sql.session.timeZone', 'UTC') \
            .config("spark.driver.memory", "16g") \
            .getOrCreate()
    else:
        spark = SparkSession.builder.appName("PreparedSourceToOHDSI") \
            .config('spark.driver.extraJavaOptions', '-Duser.timezone=GMT') \
            .config('spark.executor.extraJavaOptions', '-Duser.timezone=GMT') \
            .config('spark.sql.session.timeZone', 'UTC') \
            .getOrCreate()

    with open(arg_obj.json_generated_tables, "r") as f:
        generated_tables = json.load(f)

    with open(arg_obj.config_json_file_name, "r") as f:
        config = json.load(f)

    main(config, generated_tables)