import json
import argparse
import re


def get_table(table_dict, table_name):

    if table_name not in table_dict:
        return table_name.upper(), table_dict[table_name.upper()]
    else:
        return table_name, table_dict[table_name]


def escape_name(object_name, dialect="mssql"):
    if dialect == "mssql":
        left_escape = "["
        les = left_escape
        right_escape = "]"
        res = right_escape

    return les + object_name + res


def main(schema_dict, outfile_name, schema_name="dbo", transfer_table_prefix="transfer", columns_to_bigint=None, columns_to_expand=None,
         columns_to_trim=False, custom_field_dict=None, table_order=None, dialect="mssql"):

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
    for table in rev_table_order:
        sql_string += f"truncate table {en(sn)}.{en(table)};\n"

    for table in table_order:

        sql_string += "\n"
        sql_string += f"--Alter table {table}\n"

        tn, table_dict = get_table(schema_dict, table)

        # OHDSI CDM by default does not support BIGINT
        for column in table_dict:
            if column in columns_to_bigint:
                sql_string += f"alter table {en(sn)}.{en(tn)} alter column {en(column)} BIGINT;\n"

        # Extend length of VARCHAR to handle long descriptions found in source
        for column in table_dict:
            if column in columns_to_expand:
                sql_string += f"alter table {en(sn)}.{en(tn)} alter column {en(column)} VARCHAR(512);\n"
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
        sql_string += f"{en(sn)}.{en(transfer_table_prefix + tn)};\n\n"

    with open(outfile_name, "w") as fw:
        fw.write(sql_string)

    print(sql_string)


if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser(description="Script to generate insert statements from transferTables into OHDSI CDM")
    arg_parse_obj.add_argument("-s", "--schema-name", dest="schema_name", default="dbo")
    arg_parse_obj.add_argument("-j", "--json-schema-file", dest="json_schema_file",
                               default="../../../../src/ohdsi_datatypes_5_4.json")

    arg_parse_obj.add_argument("-o", "--output-file", dest="output_file", default="./transfer_sql_inserts_54.sql")

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
        "provider_id"
    ]

    columns_to_expand = ["value_source_value", "zip", "state", "ethnicity_source_value", "race_source_value",
                         "discharged_to_source_value", "admitted_from_source_value", "condition_status_source_value"
                         ]

    table_order = ["domain", "concept", "concept_ancestor", "concept_relationship", "drug_strength", "location",
                   "care_site", "provider", "person", "death", "observation_period", "visit_occurrence", "visit_detail",
                   "condition_occurrence", "procedure_occurrence",
                   "measurement", "observation", "drug_exposure", "device_exposure"
                   ]

    custom_field_dict = {
        "valid_start_date": "cast(cast(valid_start_date as varchar(8)) as date)",
        "valid_end_date": "cast(cast(valid_start_date as varchar(8)) as date)"
    }

    columns_to_trim = ["value_as_string"]

    main(schema_dict, arg_obj.output_file, arg_obj.schema_name, table_order=table_order,
         columns_to_bigint=columns_to_bigint, columns_to_expand=columns_to_expand, columns_to_trim=columns_to_trim,
         custom_field_dict=custom_field_dict)