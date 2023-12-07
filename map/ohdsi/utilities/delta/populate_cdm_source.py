import json
import logging
from pyspark.sql import SparkSession
import argparse
import datetime

logging.basicConfig(level=logging.INFO)
spark = SparkSession.builder.appName("PopulateCDMSource").getOrCreate()


def get_single_value(sdf):
    return sdf.toPandas().values.tolist()[0][0]


def main(database_name, metadata_json):

    spark.sql(f"use {database_name}")

    with open(metadata_json) as f:
        metadata_dict = json.load(f)

    if "vocabulary_version" not in metadata_dict or metadata_dict["vocabulary_version"] is None:
        vocabulary_version = get_single_value(spark.sql("SELECT vocabulary_version from vocabulary where vocabulary_id = 'None'"))
        print(vocabulary_version)

        metadata_dict["vocabulary_version"] = vocabulary_version

    if "cdm_version" not in metadata_dict:  # CDM v5.3.1
        raise(RuntimeError, "cdm_version is required in the metadata dict file")
    else:
        cdm_version = metadata_dict["cdm_version"]
        cdm_version_concept_id = get_single_value(spark.sql(f"SELECT * FROM CONCEPT WHERE VOCABULARY_ID = 'CDM' "
                                                            f"AND CONCEPT_CLASS_ID = 'CDM' and concept_code = '{cdm_version}'"))

        metadata_dict["cdm_version_concept_id"] = cdm_version_concept_id

    if "source_release_date" not in metadata_dict or metadata_dict["source_release_date"] is None:
        # Use last measurement date as a proxy
        source_release_date = get_single_value(spark.sql("select max(measurement_date) as last_measuremnt_date from measurement where measurement_date < now() and measurement_date is not null"))

        source_release_date_str = source_release_date.strftime("%Y-%m-%d")
        metadata_dict["source_release_date"] = source_release_date_str

    if "cdm_release_date" not in metadata_dict or metadata_dict["cdm_release_date"] is None:
        utc_now = datetime.datetime.utcnow()
        cdm_source_date = utc_now.strftime("%Y-%m-%d")

        metadata_dict["cdm_release_date"] = cdm_source_date
        print(cdm_source_date)

    if "cdm_source_name" not in metadata_dict or metadata_dict["cdm_source_name"] is None:
        metadata_dict["cdm_source_name"] = database_name

    if "cdm_source_abbreviation" not in metadata_dict or metadata_dict["cdm_source_abbreviation"] is None:
        metadata_dict["cdm_source_abbreviation"] = database_name

    spark.sql("delete from cdm_source")

    md = metadata_dict
    insert_statement = """
    insert into cdm_source (
    cdm_source_name,
    cdm_source_abbreviation,
    cdm_holder,
    source_description,
    source_documentation_reference,
    cdm_etl_reference,
    source_release_date,
    cdm_release_date,
    cdm_version,
    --cdm_version_concept_id, --5.4
    vocabulary_version
    )
    values ("""

    insert_statement += f"'{md['cdm_source_name']}', "
    insert_statement += f"'{md['cdm_source_abbreviation']}', "
    insert_statement += f"'{md['cdm_holder']}', "
    insert_statement += f"'{md['source_description']}', "
    insert_statement += f"'{md['source_documentation_reference']}', "
    insert_statement += f"'{md['cdm_etl_reference']}', "
    insert_statement += f"'{md['source_release_date']}'::date, "
    insert_statement += f"'{md['cdm_release_date']}'::date, "
    insert_statement += f"'{md['cdm_version']}', "
    #insert_statement += f"{md['cdm_version_concept_id']}, " # 5.4
    insert_statement += f"'{md['vocabulary_version']}', "

    insert_statement = insert_statement[:-2]
    insert_statement += ")"

    print(insert_statement)
    spark.sql(insert_statement)



if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser(description="Populate the CDM Source Table in the OHDSI Schema by directly connecting to the database")
    arg_parse_obj.add_argument("-n", "--database-name", dest="database_name", required=True)
    arg_parse_obj.add_argument("-m", "--populate-cdm-source-metadata-json", dest="populate_cdm_source_metadata_json",
                               required=True)

    arg_obj = arg_parse_obj.parse_args()
    main(arg_obj.database_name, arg_obj.populate_cdm_source_metadata_json)