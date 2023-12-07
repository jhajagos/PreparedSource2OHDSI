"""
    Meta-programming writes Python Script which matches the published OHDSI
    Schema.

    First run

    Build _prepared_source_sql_template.py

    This does not need to be run by the user

"""
import json


def convert_table_name_to_class_name(table_name):

    name_parts = table_name.lower().split("_")
    name_parts = [namp[0].upper() + namp[1:] for namp in name_parts]

    return "".join(name_parts) + "Object"


def main(input_json, output_python_file, ohdsi_version, indent=4):

    ohdsi_output_class_name = "OHDSI" + "".join(ohdsi_version.split(".")) + "OutputClass"

    indent_str = " " * indent

    with open(input_json) as f:
        schema_dict = json.load(f)

    with open(output_python_file, "w") as fw:

        fw.write("from preparedsource2ohdsi.base_ohdsi_cdm import OHDSIOutputClass")
        fw.write("\n\n\n")

        fw.write(f"class {ohdsi_output_class_name}(OHDSIOutputClass):\n")
        fw.write(indent_str + '"""' + f"Child Class {ohdsi_version}" + '"""\n\n')
        fw.write(indent_str + "def _version(self):\n")
        fw.write(indent_str * 2 + f'return "{ohdsi_version}"\n')

        fw.write("\n" * 2)

        for table_name in schema_dict:

            table_dict = schema_dict[table_name]

            class_name = convert_table_name_to_class_name(table_name)
            fw.write(f"class {class_name}({ohdsi_output_class_name}):\n")
            fw.write(indent_str + f"def table_name(self):\n")
            fw.write(indent_str * 2 + f'return "{table_name.lower()}"\n\n')

            column_names = list(table_dict.keys())

            fw.write(indent_str + f"def _fields(self):\n")
            fw.write(indent_str * 2 + "return " + json.dumps(column_names) + "\n\n")

            cleaned_type_table_dict = {}
            for key in table_dict:
                key_value = table_dict[key]
                if key_value.upper() == "TEXT":
                    new_key_value = "STRING"
                else:
                    new_key_value = key_value

                cleaned_type_table_dict[key] = new_key_value

            fw.write(indent_str + f"def _data_types(self):\n")
            fw.write(indent_str * 2 + "return " + json.dumps(cleaned_type_table_dict) + "\n")

            fw.write("\n\n")


main("./ohdsi_datatypes_5_3_1.json", "preparedsource2ohdsi/ohdsi_cdm_5_3_1.py", "5.3.1")

main("./ohdsi_datatypes_5_4.json", "preparedsource2ohdsi/ohdsi_cdm_5_4.py", "5.4")