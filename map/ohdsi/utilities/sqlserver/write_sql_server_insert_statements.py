import json
import argparse
import re


def get_table(table_dict, table_name, dialect="mssql"):

    if dialect == "mssql":
        if table_name not in table_dict:
            return table_name.upper(), table_dict[table_name.upper()]
        else:
            return table_name, table_dict[table_name]
    elif dialect == "psql":
        if table_name not in table_dict:
            return table_name.lower(), table_dict[table_name.upper()]
        else:
            return table_name, table_dict[table_name]


def escape_name(object_name, dialect="mssql"):
    if dialect == "mssql":
        left_escape = "["
        right_escape = "]"

    elif dialect == "psql":
        left_escape = '"'
        right_escape = '"'

    res = right_escape
    les = left_escape

    return les + object_name + res


def main(schema_dict, outfile_name, schema_name="dbo", transfer_table_prefix="transfer", columns_to_bigint=None, columns_to_expand=None,
         columns_to_trim=False, custom_field_dict=None, table_order=None, dialect="mssql", custom_where=None, concept_tables=None,
         exclude_concept_tables=False):

    if columns_to_bigint is None:
        columns_to_bigint = []

    if columns_to_trim is None:
        columns_to_trim = []

    if columns_to_expand is None:
        columns_to_expand = []

    if custom_field_dict is None:
        custom_field_dict = {}

    def en(obj_name):
        return escape_name(obj_name, dialect)

    sn = schema_name

    re_col_len = re.compile(r"VARCHAR\(([0-9]+)\)")

    sql_string = "/* Script to load transferTables to a standard OHDSI CDM */"
    sql_string += "\n\n\n"

    sql_string += "--Truncate tables"
    sql_string += "\n"
    rev_table_order = reversed(table_order)

    if dialect == "psql":
        custom_field_dict["quantity"] = "cast(quantity as float)"

    for table in rev_table_order:

        truncate_table = True
        if table in concept_tables and exclude_concept_tables is True:
            truncate_table = False

        if truncate_table:
            tn, _ = get_table(schema_dict, table, dialect)
            sql_string += f"truncate table {en(sn)}.{en(tn)};\n"

    for table in table_order:

        write_table = True
        if table in concept_tables and exclude_concept_tables is True:
            write_table = False

        if write_table:
            sql_string += "\n"
            sql_string += f"--Alter table {table}\n"

            tn, table_dict = get_table(schema_dict, table, dialect)

            # OHDSI CDM by default does not support BIGINT
            for column in table_dict:
                if column in columns_to_bigint:
                    if dialect == "mssql":
                        sql_string += f"alter table {en(sn)}.{en(tn)} alter column {en(column)} BIGINT;\n"
                    else:
                        sql_string += f"alter table {en(sn)}.{en(tn)} alter column {en(column)} type BIGINT;\n"

            # Extend length of VARCHAR to handle long descriptions found in source
            for column in table_dict:
                if column in columns_to_expand:
                    if dialect == "mssql":
                        sql_string += f"alter table {en(sn)}.{en(tn)} alter column {en(column)} VARCHAR(512);\n"
                    else:
                        sql_string += f"alter table {en(sn)}.{en(tn)} alter column {en(column)} type VARCHAR(512);\n"
                    table_dict[column] = "VARCHAR(512)"

            sql_string += "\n"
            sql_string += f"insert into {en(sn)}.{en(tn)}"
            column_str = ""
            for column in table_dict:
                column_str += f"{column},"

            column_str = column_str[:-1]
            sql_string += f" ({column_str})\n"
            sql_string += f"select "

            select_str = ""
            for column in table_dict:

                if dialect == "psql":
                    if "datetime" in column:
                        custom_field_dict[column] = f"cast({column} as timestamp)"

                if column in custom_field_dict:
                    select_str += f"{custom_field_dict[column]},"
                elif column in columns_to_trim:
                    data_type = table_dict[column]
                    g = re_col_len.match(data_type).groups()
                    field_len = g[0]
                    select_str += f"left({en(column)},{field_len}),"
                else:
                    select_str += f"{en(column)},"
            select_str = select_str[:-1]

            select_str += " "
            sql_string += select_str
            sql_string += "\n    from "
            if dialect == "mssql":
                sql_string += f"{en(sn)}.{en(transfer_table_prefix + tn)}"
            elif dialect == "psql":
                sql_string += f"{en(sn)}.{en(transfer_table_prefix + tn)}"

            if table in custom_where:
                sql_string += f"\n where {custom_where[table]}"

            sql_string += ";\n\n"

    with open(outfile_name, "w") as fw:
        fw.write(sql_string)

    print(sql_string)


if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser(description="Script to generate insert statements from transferTables into OHDSI CDM")
    arg_parse_obj.add_argument("-s", "--schema-name", dest="schema_name", default="dbo")
    arg_parse_obj.add_argument("-j", "--json-schema-file", dest="json_schema_file",
                               default="../../../../src/ohdsi_datatypes_5_4.json")

    arg_parse_obj.add_argument("-d", "-d", "--dialect", dest="sql_dialect", default="mssql", help="Currently supported: psql (postgresql) & mssql (Microsoft SQL Server)")

    arg_parse_obj.add_argument("-o", "--output-file", dest="output_file", default="./transfer_sql_inserts_54.sql")

    arg_parse_obj.add_argument("-x", "--exclude-concept-tables", dest='exclude_concept_tables', default=False, action="store_true")

    arg_obj = arg_parse_obj.parse_args()

    with open(arg_obj.json_schema_file) as f:
        schema_dict = json.load(f)

    columns_to_bigint = [
        "visit_occurrence_id",
        "person_id",
        "condition_occurrence_id",
        "observation_id",
        "measurement_id",
        "observation_period_id",
        "procedure_occurrence_id",
        "visit_detail_id",
        "drug_exposure_id",
        "note_id",
        "note_nlp_id",
        "device_exposure_id",
        "provider_id",
        "payer_plan_period_id",
        "location_id",
        "subject_id",
        "fact_id_1",
        "fact_id_2"
    ]

    columns_to_expand = ["value_source_value", "zip", "state", "ethnicity_source_value", "race_source_value",
                         "discharged_to_source_value", "admitted_from_source_value", "condition_status_source_value",
                         "drug_source_value", "payer_source_value", "address_1", "address_2", "country_source_value",
                         "county"
                         ]

    table_order = ["domain", "concept_class", "concept", "vocabulary", "concept_ancestor",
                   "relationship", "concept_relationship", "drug_strength",
                   "cdm_source", "location",
                   "care_site", "provider", "person", "death", "observation_period", "visit_occurrence", "visit_detail",
                   "condition_occurrence", "procedure_occurrence",
                   "measurement", "observation", "drug_exposure", "device_exposure",
                   "payer_plan_period", "fact_relationship"
                   ]

    concept_tables = [
        "vocabulary", "concept_class", "concept",  "concept_ancestor", "relationship", "concept_relationship", "drug_strength", "domain"
    ]

    custom_field_dict = {
        "valid_start_date": "cast(cast(valid_start_date as varchar(8)) as date)",
        "valid_end_date": "cast(cast(valid_start_date as varchar(8)) as date)",
        "drug_exposure_end_date": "coalesce(drug_exposure_end_date, drug_exposure_start_date)",
        "latitude": "cast(latitude as float)", "longitude": "cast(longitude as float)"
    }

    custom_where = {
        "drug_exposure": "drug_exposure_start_date is not NULL"
    }

    columns_to_trim = ["value_as_string", "drug_source_value", "payer_source_value"]

    main(schema_dict, arg_obj.output_file, arg_obj.schema_name, table_order=table_order,
         columns_to_bigint=columns_to_bigint, columns_to_expand=columns_to_expand, columns_to_trim=columns_to_trim,
         custom_field_dict=custom_field_dict, dialect=arg_obj.sql_dialect, custom_where=custom_where,
         concept_tables=concept_tables, exclude_concept_tables=arg_obj.exclude_concept_tables)