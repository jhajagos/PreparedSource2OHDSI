from pyspark.sql import SparkSession
import argparse
import json
import pprint
from pyspark import SparkConf


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

    arg_parser_obj.add_argument("-j", "--json-generated-tables", dest="json_generated_tables")
    arg_parser_obj.add_argument("-c", "--config-json-file-name", dest="config_json_file_name")
    arg_parser_obj.add_argument("-s", "--schema", dest="schema_name", default=None)
    arg_parser_obj.add_argument("-l", "--run-local", dest="run_local", default=False, action="store_true")
    arg_parser_obj.add_argument("-x", "--exclude-concept-tables", dest="exclude_concept_tables", default=False, action="store_true")
    arg_parser_obj.add_argument("--exclude-prepared-source-tables", dest="exclude_prepared_source_tables", default=False, action="store_true")
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

    spark = SparkSession.builder.config(conf=sconf).appName("StageStreamlinedPreparedSource").getOrCreate()

    with open(arg_obj.json_generated_tables, "r") as f:
        generated_tables = json.load(f)

    with open(arg_obj.config_json_file_name) as f:
        config = json.load(f)

    main(config, generated_tables, schema=arg_obj.schema_name,
         exclude_concepts=arg_obj.exclude_concept_tables,
         exclude_prepared_source_tables=arg_obj.exclude_prepared_source_tables)