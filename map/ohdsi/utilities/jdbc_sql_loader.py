import json
import jaydebeapi
import sqlparse
import argparse

def main(sql_filename, connection_string, username, password, class_path, jar_path, substitution):
    conn = jaydebeapi.connect(class_path, connection_string, [username, password], jar_path)

    print(f"Reading '{sql_filename}'")

    if substitution is not None:
        if "|" not in substitution:
            raise RuntimeError(f"substitution must contain a '|' character")
        else:
            find_str, replace_str = substitution.split("|")

    with open(sql_filename, "r") as f:
        sql_file = f.read()

    sql_statements = sqlparse.split(sql_file)

    cursor = conn.cursor()

    for statement in sql_statements:

        if substitution is not None:
            if find_str in statement:
                statement = statement.replace(find_str, replace_str)

        print(statement)
        cursor.execute(statement)


if __name__ == "__main__":

    argparse_obj = argparse.ArgumentParser(description="Executes a SQL script using a JDBC driver")
    argparse_obj.add_argument("-f", "--sql-file-name", dest="sql_file_name")
    argparse_obj.add_argument("-c", "--class-path", dest="class_path", default="com.microsoft.sqlserver.jdbc.SQLServerDriver")
    argparse_obj.add_argument("-p", "--path-to-jdbc-jar", dest="jar_path", default="/root/jdbc/mssql-jdbc-12.6.2.jre11.jar")
    argparse_obj.add_argument("--config-json-filename", dest="config_json_filename", default="/root/config/prepared_source_to_ohdsi_config.json")
    argparse_obj.add_argument("--substitution", dest="substitution", help="Substitute a string for example '@cdmDatabaseSchema|ohdsi'")

    arg_obj = argparse_obj.parse_args()

    with open(arg_obj.config_json_filename) as f:
        c = json.load(f)

    username = c["jdbc"]["properties"]["username"]
    password = c["jdbc"]["properties"]["password"]
    connection_string = c["jdbc"]["connection_string"]

    main(arg_obj.sql_file_name, connection_string, username, password, arg_obj.class_path, arg_obj.jar_path,
         arg_obj.substitution)

