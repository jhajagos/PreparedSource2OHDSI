import argparse

def main():
    pass

arg_parse_obj = argparse.ArgumentParser(description="Swiss army knife script for transforming prepared source from CSV to OHDSI")

arg_parse_obj.add_argument("-c", "--commands", nargs="*", help="The following commands can be run: 'intiliaze_concepts', 'ps2parquet' 'ps2ohdsi', 'create_ohdsi_schema', 'stage_tables_db', 'load_ohdsi_tables'")
arg_parse_obj.add_argument("--concepts-reload", nargs="*", help="Processing the concept", default=False, action="store_true")
arg_parse_obj.add_argument("-t", "--test-configuration", help="Will run through a list of checkpoints to make you have configured the checks")

if __name__ == "__main__":

    main()