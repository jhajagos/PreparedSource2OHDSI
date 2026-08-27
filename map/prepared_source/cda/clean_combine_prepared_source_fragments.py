import argparse
import json
import csv
import pathlib
from preparedsource2ohdsi.prepared_source import SourceObservationPeriodObject
import os

def read_unit_map(file_name='./mappings/units_mapping.csv'):

    with open(file_name, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        unit_map = {}
        for row in reader:
            unit_map[row["s_unit_code"]] = row["m_unit_code"]
        return unit_map

def write_row(cvr, row_dict,  header):
    """Write a row"""
    row_to_write = [''] * len(header)
    for column in row_dict:
        position = header.index(column)
        row_to_write[position] = row_dict[column]
    cvr.writerow(row_to_write)


def get_union_csv_file_header(csv_files):
    """Union columns across all files"""
    header_union = []
    for csv_file in csv_files:
        with open(csv_file, newline="", mode="r") as f:
            cvr = csv.reader(f)
            local_header = next(cvr)
            for column in local_header:
                if column not in header_union:
                    header_union += [column]

    return header_union


def load_and_merge_manifests(directories):
    """Load each directory's output/s_files_generated.json and merge them into one manifest.

    All directories must declare the same s_person_id (they are different extractions -- e.g. C-CDA
    and FHIR -- for the *same* person); this is asserted rather than silently allowed so that mixing
    manifests for different people fails loudly instead of quietly producing a duplicate OMOP person.
    Fragment lists are concatenated in the order directories are given, which matters for
    'source_person': the combine step below keeps the first non-empty value seen per column, so the
    first directory's source_person fragment(s) take priority over later ones.
    """
    manifests = []
    for directory in directories:
        json_file_path = pathlib.Path(directory) / "output" / "s_files_generated.json"
        with open(json_file_path, "r") as f:
            manifests.append(json.load(f))

    person_ids = {m["s_person_id"] for m in manifests}
    if len(person_ids) > 1:
        raise ValueError(
            f"Manifests being combined declare different s_person_id values ({sorted(person_ids)}); "
            f"refusing to merge fragments for different people into one person. Directories: {directories}")

    merged_fragments = {}
    for manifest in manifests:
        for table_name, fragment_paths in manifest["fragments"].items():
            merged_fragments.setdefault(table_name, [])
            merged_fragments[table_name] += fragment_paths

    return {"s_person_id": person_ids.pop(), "prepared_source": {}, "fragments": merged_fragments}


def main(directories, output_directory=None):

    if isinstance(directories, str):
        directories = [directories]

    s_generation_dict = load_and_merge_manifests(directories)
    s_person_id = s_generation_dict["s_person_id"]

    output_p_directory = pathlib.Path(output_directory) if output_directory else pathlib.Path(directories[0])
    output_directory_root = output_p_directory / "output"
    ps_path = output_directory_root / "ps"
    os.makedirs(str(ps_path), exist_ok=True)

    min_datetime = "2200-01-01"
    max_datetime = "1900-01-01"

    date_to_evaluate = None

    unit_map = read_unit_map(os.path.join(os.path.dirname(__file__), "./mappings/units_mapping.csv"))

    print("Consolidating 'source_person.csv'")
    # We can only have one row per person, so we only add values which have not been set yet --
    # i.e. the first fragment (in manifest/directory order) to supply a non-empty value for a
    # column wins, later fragments only fill in columns still blank.
    source_person_header = get_union_csv_file_header(s_generation_dict["fragments"]["source_person"])
    source_person_dict = {sp: "" for sp in source_person_header}
    for file in s_generation_dict["fragments"]["source_person"]:
        print(f"Reading '{file}'")
        with open(file, newline="", mode="r") as f:
            cdr = csv.DictReader(f)
            first_row = next(cdr)
            for column in first_row:
                if len(first_row[column]) > 0 and len(source_person_dict[column]) == 0:
                    source_person_dict[column] = first_row[column]

    # TODO: Add standard maps to OHDSI for race, ethnicity, and gender
    source_person_path = ps_path / "source_person.csv"
    print(f"Writing 1 row in '{source_person_path}'")
    header = list(source_person_dict.keys())

    # Write source person
    with open(source_person_path, mode="w", newline="") as fw:
        cdw = csv.writer(fw)
        cdw.writerow(header)
        write_row(cdw, source_person_dict, header)

    s_generation_dict["prepared_source"]["source_person"] = source_person_path

    source_fragments = list(s_generation_dict["fragments"])
    source_fragments = list(set(source_fragments) - set(["source_person"]))

    for source_fragment in source_fragments:
        fragment_paths = s_generation_dict["fragments"][source_fragment]
        source_file_name = source_fragment + ".csv"
        source_path = ps_path / source_file_name
        source_header = get_union_csv_file_header(fragment_paths)

        with open(source_path, newline="", mode="w", encoding="utf-8", errors="replace") as fw:
            cdw = csv.writer(fw)
            cdw.writerow(source_header)
            i = 0
            for fragment in fragment_paths:
                print(f"Reading '{fragment}'")
                with open(fragment, "r", newline="", encoding="utf-8", errors="replace") as f:
                    cdr = csv.DictReader(f)

                    for row_dict in cdr:

                        if source_fragment == "source_result":

                            s_obtained_datetime = row_dict["s_obtained_datetime"]
                            s_result_datetime = row_dict["s_result_datetime"]

                            if len(s_obtained_datetime) == 0 and len(s_result_datetime) > 0:
                                date_to_evaluate = s_result_datetime
                            elif len(s_obtained_datetime) > 0 and len(s_result_datetime) == 0:
                                date_to_evaluate = s_obtained_datetime

                            elif len(s_obtained_datetime) == 0 and len(s_result_datetime) == 0:
                                date_to_evaluate = None
                                row_dict["i_exclude"] = 0

                            if row_dict["s_result_unit_code"] in unit_map: # Clean up non-standard or UCUM codes that do not map to OHDSI CDM
                                row_dict["m_result_unit_code"] = unit_map[row_dict["s_result_unit_code"]]
                                row_dict["m_result_unit_code_type"] = row_dict["s_result_unit_code_type"]
                                row_dict["m_result_unit_code_type_oid"] = row_dict["s_result_unit_code_type_oid"]

                        elif source_fragment == "source_procedure":

                            s_start_procedure_datetime = row_dict["s_start_procedure_datetime"]
                            if len(s_start_procedure_datetime):
                                date_to_evaluate = s_start_procedure_datetime
                            else:
                                date_to_evaluate = None

                        elif source_fragment == "source_condition":

                            s_start_condition_datetime = row_dict["s_start_condition_datetime"]
                            s_end_condition_datetime = row_dict["s_end_condition_datetime"]

                            if len(s_start_condition_datetime) == 0 and len(s_end_condition_datetime) > 0:

                                s_start_condition_datetime = s_end_condition_datetime
                                row_dict["s_start_condition_datetime"] = s_start_condition_datetime
                                date_to_evaluate = s_start_condition_datetime

                            elif len(s_start_condition_datetime) == 0 and len(s_end_condition_datetime) == 0:
                                date_to_evaluate = None
                                row_dict["i_exclude"] = 1


                            else:
                                date_to_evaluate = s_start_condition_datetime

                        elif source_fragment == "source_medication":
                            s_start_medication_datetime = row_dict["s_start_medication_datetime"]
                            s_end_medication_datetime = row_dict["s_end_medication_datetime"]

                            if len(s_start_medication_datetime) == 0 and len(s_end_medication_datetime) > 0:

                                row_dict["s_start_medication_datetime"] = row_dict[
                                    "s_end_medication_datetime"]
                                s_start_medication_datetime = row_dict["s_start_medication_datetime"]

                                date_to_evaluate = s_start_medication_datetime

                            elif len(s_start_medication_datetime) > 0 :
                                date_to_evaluate = s_start_medication_datetime
                            else:
                                date_to_evaluate = None
                                row_dict["i_exclude"] = 1

                        if date_to_evaluate is not None:
                            if len(date_to_evaluate) > 0:
                                if date_to_evaluate[0:10] < min_datetime:
                                    min_datetime = date_to_evaluate[0:10]

                                if date_to_evaluate[0:10] > max_datetime:
                                    max_datetime = date_to_evaluate[0:10]
                            else:
                                print(row_dict)
                                print(source_fragment)
                                raise RuntimeError


                        write_row(cdw, row_dict, source_header)
                        i += 1

            print(f"Writing {i} rows in '{source_path}'" )

    # Use the manifest's s_person_id rather than whatever row_dict happens to hold last -- some
    # tables (source_location, source_care_site, source_provider) have no s_person_id field at all,
    # so relying on a leftover loop variable is unsafe once those tables are part of the merge.
    source_observation_period_dict = SourceObservationPeriodObject().dict_template()

    source_observation_period_dict["s_person_id"] = s_person_id
    source_observation_period_dict["s_start_observation_datetime"] = min_datetime
    source_observation_period_dict["s_end_observation_datetime"] = max_datetime

    source_observation_period_dict['m_source'] = 'EHR'
    source_observation_period_dict['m_source_code'] = 'OMOP4976890'
    source_observation_period_dict['m_source_code_type'] = 'OHDSI'
    source_observation_period_dict['m_source_code_type_oid'] = 'ohdsi.type_concept'

    source_ob_header = list(source_observation_period_dict.keys())

    source_ob_path = ps_path / "source_observation_period.csv"
    print(f"Writing 1 row in '{source_ob_path}'")
    with open(source_ob_path, newline="", mode="w") as fw:
        cdw = csv.writer(fw)
        cdw.writerow(source_ob_header)
        write_row(cdw, source_observation_period_dict, source_ob_header)


if __name__ == "__main__":
    arg_parse_obj = argparse.ArgumentParser(
        description="Combine fragments generated by 'cda_to_prepared_source_fragments.py' and/or "
                     "'fhir_to_prepared_source_fragments.py' into a single set of files. Pass -d more "
                     "than once to merge fragments from multiple sources (e.g., C-CDA and FHIR) for the "
                     "same person -- all directories must share the same manifest s_person_id.")
    arg_parse_obj.add_argument("-d", "--directory", dest="directories", action="append", default=None,
                               help="A directory containing an output/s_files_generated.json manifest; "
                                    "repeat to merge multiple sources.")
    arg_parse_obj.add_argument("-o", "--output-directory", dest="output_directory", default=None,
                               help="Directory to write the combined output/ps/*.csv into. Defaults to "
                                    "the single input directory when only one -d is given.")
    arg_obj = arg_parse_obj.parse_args()

    directories = arg_obj.directories or ["./test/samples/patient_1/"]
    main(directories, arg_obj.output_directory)