import json
import shutil
import glob
import argparse


def main(jdbc_connection_string, username, password):

    copy_jar_files()

    with open("/root/config/prepared_source_to_ohdsi_config.json") as f:
        config = json.load(f)

    config["jdbc"]["connection_string"] = jdbc_connection_string
    config["jdbc"]["properties"]["username"] = username
    config["jdbc"]["properties"]["password"] = password

    with open("/root/config/prepared_source_to_ohdsi_config.json", "w") as fw:
        json.dump(config, fw)


def copy_jar_files():

    jar_files = glob.glob("/root/jdbc/*.jar")

    for jar_file in jar_files:
        destination = "/root/miniconda3/envs/PySpark/lib/python3.9/site-packages/pyspark/jars/"
        print(f"Copying '{jar_file}' to '{destination}'")
        shutil.copy(jar_file, destination)


if __name__ == "__main__":

    arg_parser_obj = argparse.ArgumentParser(description="Adds JDBC connection information")

    arg_parser_obj.add_argument("-j", "--jdbc-connection-string", dest="jdbc_connection_string")
    arg_parser_obj.add_argument("-u", "--username", dest="username")
    arg_parser_obj.add_argument("-p", "--password", dest="password")

    arg_parse = arg_parser_obj.parse_args()

    main(arg_parse.jdbc_connection_string, arg_parse.username, arg_parse.password)

