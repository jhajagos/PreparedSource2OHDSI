import sqlalchemy as sa
import json
import requests
import sqlparse # Used to parse the SQL file


def main(ddl_link, version="5.3.1"):
    """Builds a JSON file for automating the building of an OHDSI table"""

    ddl_request = requests.get(ddl_link)

    ddl = ddl_request.content

    version_name = "_".join(version.split("."))

    engine = sa.create_engine("sqlite://")  # We load the schema into an in-memory SQLite
    with engine.connect() as connection:
        for statement in sqlparse.split(ddl):
            sqlite_statement = sa.text(statement.replace("@cdmDatabaseSchema.", ""))
            connection.execute(sqlite_statement)

        meta_data = sa.MetaData()
        meta_data.reflect(bind=connection)

        table_dict = {}
        for table in meta_data.tables:
            table_obj = meta_data.tables[table]
            column_dict = {}
            for column_obj in table_obj.columns:
                column_dict[column_obj.name] = str(column_obj.type)
            table_dict[table] = column_dict

        with open(f"ohdsi_datatypes_{version_name}.json", mode="w") as fw:
            json.dump(table_dict, fw)

main("https://raw.githubusercontent.com/OHDSI/CommonDataModel/v5.4.0/inst/ddl/5.3/postgresql/OMOPCDM_postgresql_5.3_ddl.sql")

main("https://raw.githubusercontent.com/OHDSI/CommonDataModel/v5.4.0/inst/ddl/5.4/postgresql/OMOPCDM_postgresql_5.4_ddl.sql", "5.4")