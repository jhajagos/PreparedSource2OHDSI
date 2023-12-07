"""
    Builds a template for SQL with documentation for ETL writer:

    --
    select
        null as s_person_id
        ....
        ,null as i_exclude
    from SourcePerson
    ;

    This needs to be run each time the prepared_source classes are open

"""

import preparedsource2ohdsi.prepared_source as prepared_source


def main(outfile_name):

    potential_ps_classes = dir(prepared_source)
    ps_classes = [cn for cn in potential_ps_classes if "Source" in cn and "Object" in cn != "PreparedSourceObject"]

    sql_string = ""
    sql_string += """/*
    Template to be modified for SQL ETL to map source data to the prepared source format
    
    Definitions:
    
    OID = Object Identifier https://www.hl7.org/oid/
    ETL = Extract Transform Load
    
    Key:
    
    s_ field prefix indicates source value representation
    m_ field prefix indicates value has been mapped / transformed
    k_ field prefix indicates a key value for linking to another table
    
    _code field suffix indicates value is coded
    _code_type field suffix is the human readable description
    _code_type_oid field suffix is the OID value of the field
*/
"""
    for ps_class in ps_classes:
        ps_obj = eval("prepared_source" + "." + ps_class + "()")
        field_names = ps_obj.fields()
        meta_data = ps_obj.meta_data()

        sql_string += '\n\nselect'
        i = 0

        for field_name in field_names:

            sql_string += "\n   "
            if i > 0:
                sql_string += ","
            else:
                sql_string += " "

            sql_string += f"NULL as {field_name}"
            if field_name in meta_data:
                sql_string += f" --{meta_data[field_name]}"
            i += 1

        sql_string += f"\nfrom {ps_class};"

    print(sql_string)

    with open(outfile_name, "w") as fw:
        fw.write(sql_string)


if __name__ == "__main__":
    main("./prepared_source_sql_template.sql")