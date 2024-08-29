import json
import xml.etree.ElementTree as et
import hashlib
import os
import pathlib
import csv
import json
import preparedsource2ohdsi.prepared_source as ps
import argparse
import glob

CDANS = "{urn:hl7-org:v3}"


def code_to_dict(element):
    code_dict = {}
    if "code" in element.attrib:
        code_dict["s_code"] = element.attrib["code"]
    else:
        code_dict["s_code"] = None

    if "codeSystem" in element.attrib:
        code_dict["s_code_type_oid"] = element.attrib["codeSystem"]
    else:
        code_dict["s_code_type_oid"] = None

    if "codeSystemName" in element.attrib:
        code_dict["s_code_type"] = element.attrib["codeSystemName"]
    else:
        code_dict["s_code_type"] = None

    for child in element:
        if child.tag == ext("originalText"):
            code_dict["s_text"] = child.text
        else:
            code_dict["s_text"] = None

    return code_dict

def expand_tag(tag_name):
    return f"{CDANS}{tag_name}"


ext = expand_tag


def clean_datetime(datetime_str):
    # TODO: Implement consistent formatting
    return datetime_str


# cda = et.parse(cda_filename)
# observations = list(cda.iterfind("./{urn:hl7-org:v3}entry/{urn:hl7-org:v3}organizer/{urn:hl7-org:v3}component/{urn:hl7-org:v3}observation"))
def extract_source_person_ccda(xml_doc, source_person_id, source_cda_file_name):
    """Extract details for source_person from the C-CDA the patientRole section"""
    # source_person

    # ./ClinicalDocument/recordTarget/patientRole/patient/birthTime
    # ./ClinicalDocument/recordTarget/patientRole/patient/administrativeGenderCode
    # ./ClinicalDocument/recordTarget/patientRole/patient/raceCode
    # ./ClinicalDocument/recordTarget/patientRole/patient/ethnicGroupCode
    # ./ClinicalDocument/recordTarget/patientRole/patient/sdtc:deceasedInd
    # ./ClinicalDocument/recordTarget/patientRole/patient/sdtc:deceasedTime

    root = xml_doc.getroot()
    find_person_xpath = './/{urn:hl7-org:v3}recordTarget/{urn:hl7-org:v3}patientRole/.'

    source_person_obj = ps.SourcePersonObject()
    source_person_dict = source_person_obj.dict_template()
    source_person_dict["s_person_id"] = source_person_id

    for element in root.iterfind(find_person_xpath):
        if element.tag == ext("patientRole"):
            for child in element:
                if child.tag == ext("id"):
                    if "root" in child.attrib:
                        source_person_dict["s_id"] = child.attrib["root"]
                elif child.tag == ext("patient"):
                    for grandchild in child:
                        if grandchild.tag == ext("administrativeGenderCode"):
                            gender_code_dict = code_to_dict(grandchild)
                            source_person_dict["s_gender_code"] = gender_code_dict["s_code"]
                            source_person_dict["s_gender_code_type_oid"] = gender_code_dict["s_code_type_oid"]
                            source_person_dict["s_gender_code_type"] = gender_code_dict["s_code_type"]
                            source_person_dict["s_gender"] = gender_code_dict["s_text"]

                        elif grandchild.tag == ext("raceCode"):
                            race_code_dict = code_to_dict(grandchild)
                            source_person_dict["s_race_code"] = race_code_dict["s_code"]
                            source_person_dict["s_race_code_type_oid"] = race_code_dict["s_code_type_oid"]
                            source_person_dict["s_race_code_type"] = race_code_dict["s_code_type"]
                            source_person_dict["s_race"] = race_code_dict["s_text"]

                        elif grandchild.tag == ext("ethnicGroupCode"):
                            ethnic_code_dict = code_to_dict(grandchild)
                            source_person_dict["s_ethnicity_code"] = ethnic_code_dict["s_code"]
                            source_person_dict["s_ethnicity_code_type_oid"] = ethnic_code_dict["s_code_type_oid"]
                            source_person_dict["s_ethnicity_code_type"] = ethnic_code_dict["s_code_type"]
                            source_person_dict["s_ethnicity"] = ethnic_code_dict["s_text"]

                        elif grandchild.tag == ext("birthTime"):
                            if "value" in grandchild.attrib:
                                source_person_dict["s_birth_datetime"] = clean_datetime(grandchild.attrib["value"])
    return [source_person_dict]
def extract_source_provider_ccda(xml_doc):
    # source_provider
    # ./ClinicalDocument/documentationOf/serviceEvent/performer
    # ./ClinicalDocument/component/structuredBody/component/section/code --Get Section

    source_provider_obj = ps.SourceProviderObject()


def extract_problems_source_condition_ccda(xml_doc):
    # Active problem lists
    # /ClinicalDocument/component/structuredBody/component/section/entry/act/entryRelationship/observation/code[@code="404684003"][@codeSystem="2.16.840.1.113883.6.96"]/..

    source_condition_obj = ps.SourceConditionObject()


def extract_source_procedures_ccda(xml_doc):
    # Procedures
    # /ClinicalDocument/component/structuredBody/component/section/code[@code="47519-4"][@codeSystem="2.16.840.1.113883.6.1"]/../entry/observation
    # /ClinicalDocument/component/structuredBody/component/section/code[@code="47519-4"][@codeSystem="2.16.840.1.113883.6.1"]/../entry/procedure

    source_procedure_obj = ps.SourceProviderObject()


def extract_source_encounter_ccda(xml_doc):
    # Encounters
    # /ClinicalDocument/component/structuredBody/component/section/code[@code="46240-8"][@codeSystem="2.16.840.1.113883.6.1"]/../entry/encounter

    source_encounter_obj = ps.SourceEncounterObject()


def extract_labs_source_result_ccda(xml_doc, source_person_id, source_cda_file_name):
    # Labs
    # /ClinicalDocument/component/structuredBody/component/section/code[@code="30954-2"][@codeSystem="2.16.840.1.113883.6.1"]/../entry/organizer/component/observation
    # .//{urn:hl7-org:v3}structuredBody/{urn:hl7-org:v3}component/{urn:hl7-org:v3}section/{urn:hl7-org:v3}code[@code="30954-2"][@codeSystem="2.16.840.1.113883.6.1"]/../{urn:hl7-org:v3}entry/{urn:hl7-org:v3}organizer/{urn:hl7-org:v3}component/{urn:hl7-org:v3}observation/
    root = xml_doc.getroot()
    find_labs_xpath = './/{urn:hl7-org:v3}structuredBody/{urn:hl7-org:v3}component/{urn:hl7-org:v3}section/{urn:hl7-org:v3}code[@code="30954-2"][@codeSystem="2.16.840.1.113883.6.1"]/../{urn:hl7-org:v3}entry/{urn:hl7-org:v3}organizer/{urn:hl7-org:v3}component/{urn:hl7-org:v3}observation/.'

    source_result_obj = ps.SourceResultObject()

    result_list = []
    for element in root.iterfind(find_labs_xpath):
        source_result_dict = source_result_obj.dict_template()
        source_result_dict["s_person_id"] = source_person_id
        source_result_dict["s_source_system"] = f"c-cda/{source_cda_file_name}"

        for child in element:
            if child.tag == ext("id"):
                if "root" in child.attrib:
                    source_result_dict["s_id"] = child.attrib["root"]

            elif child.tag == ext("effectiveTime"):
                if "value" in child.attrib:
                    source_result_dict["s_result_datetime"] = clean_datetime(child.attrib["value"])

            elif child.tag == ext("code"):

                if "code" in child.attrib:
                    source_result_dict["s_code"] = child.attrib["code"]

                if "codeSystem" in child.attrib:
                    source_result_dict["s_code_type_oid"] = child.attrib["codeSystem"]

                if "codeSystemName" in child.attrib:
                    source_result_dict["s_code_type"] = child.attrib["codeSystemName"]

                for grandchild in child:
                    if grandchild.tag == ext("originalText"):
                        source_result_dict["s_name"] = grandchild.text

            elif child.tag == ext("interpretationCode"):

                if "code" in child.attrib:
                    source_result_dict["s_result_code"] = child.attrib["code"]

                if "codeSystem" in child.attrib:
                    source_result_dict["s_result_code_type_oid"] = child.attrib["codeSystem"]

                if "codeSystemName" in child.attrib:
                    source_result_dict["s_result_code_type"] = child.attrib["codeSystemName"]

                for grandchild in child:
                    if grandchild.tag == ext("originalText"):
                        source_result_dict["s_result_text"] = grandchild.text

            elif child.tag == ext("value"):
                if "{http://www.w3.org/2001/XMLSchema-instance}type" in child.attrib:
                    value_type = child.attrib["{http://www.w3.org/2001/XMLSchema-instance}type"]

                    if value_type == "PQ":

                        if "value" in child.attrib:
                            source_result_dict["s_result_number"] = child.attrib["value"]

                        if "unit" in child.attrib:
                            source_result_dict["s_result_unit"] = child.attrib["unit"]


            elif child.tag == ext("referenceRange"):
                # TODO: Implemnt parsing of reference range
                pass


        result_list += [source_result_dict]

    return result_list


def extract_source_medication_ccda(xml_doc, source_person_id, source_cda_file_name):
    """Extract medications from the medications section of C-CDA document"""
    # Medications
    # /ClinicalDocument/component/structuredBody/component/section/code[@code="10160-0"][@codeSystem="2.16.840.1.113883.6.1"]/../entry/substanceAdministration
    # .//{urn:hl7-org:v3}structuredBody/{urn:hl7-org:v3}component/{urn:hl7-org:v3}section/{urn:hl7-org:v3}code[@code="10160-0"][@codeSystem="2.16.840.1.113883.6.1"]/../{urn:hl7-org:v3}entry/{urn:hl7-org:v3}substanceAdministration

    source_med_obj = ps.SourceMedicationObject()
    root = xml_doc.getroot()
    find_meds_xpath = './/{urn:hl7-org:v3}structuredBody/{urn:hl7-org:v3}component/{urn:hl7-org:v3}section/{urn:hl7-org:v3}code[@code="10160-0"][@codeSystem="2.16.840.1.113883.6.1"]/../{urn:hl7-org:v3}entry/{urn:hl7-org:v3}substanceAdministration'

    result_list = []
    for element in root.iterfind(find_meds_xpath):

        source_med_dict = source_med_obj.dict_template()
        source_med_dict["s_person_id"] = source_person_id
        source_med_dict["s_source_system"] = f"c-cda/{source_cda_file_name}"

        for child in element:
            if child.tag == ext("id"):
                if "root" in child.attrib:
                    source_med_dict["s_id"] = child.attrib["root"]
            elif child.tag == ext("effectiveTime"):

                """
                TODO: Handle these cases
                <effectiveTime xsi:type="IVL_TS" operator="I">
                    <high value="20221024000000-0000" inclusive="true" />
                  </effectiveTime>
                 
                <effectiveTime xsi:type="IVL_TS" operator="I">
                    <low value="20221024000000-0000" inclusive="true" />
                    <high nullFlavor="UNK" inclusive="true" />
                </effectiveTime>
                """

                for grandchild in child:
                    if grandchild.tag == ext("low"):
                        if "value" in grandchild.attrib:
                            source_med_dict["s_start_medication_datetime"] = clean_datetime(grandchild.attrib["value"])
                    elif grandchild.tag == ext("high"):
                        if "value" in grandchild.attrib:
                            source_med_dict["s_end_medication_datetime"] = clean_datetime(grandchild.attrib["value"])
            elif child.tag == ext("statusCode"):
                if "code" in child.attrib:
                    source_med_dict["s_status"] = child.attrib["code"]

            elif child.tag == ext("routeCode"):
                if "code" in child.attrib:
                    source_med_dict["s_route_code"] = child.attrib["code"]

                if "codeSystemName" in child.attrib:
                    source_med_dict["s_route_code_type"] = child.attrib["codeSystemName"]

                if "codeSystem" in child.attrib:
                    source_med_dict["s_route_code_type_oid"] = child.attrib["codeSystem"]

                for grandchild in child:
                    if grandchild.tag == ext("originalText"):
                        source_med_dict["s_route"] = grandchild.text

            elif child.tag == ext("doseQuantity"):
                if "value" in child.attrib:
                    source_med_dict["s_quantity"] = child.attrib["value"]
                if "unit" in child.attrib:
                    source_med_dict["s_dose_unit"] = child.attrib["unit"]

            elif child.tag == ext("consumable"):
                for grandchild in child:
                    if grandchild.tag == ext("manufacturedProduct"):
                        for greatgrandchild in grandchild:
                            if greatgrandchild.tag == ext("manufacturedMaterial"):
                                for greatgreatgrandchild in greatgrandchild:
                                    if greatgreatgrandchild.tag == ext("code"):
                                        if "code" in greatgreatgrandchild.attrib:
                                            source_med_dict["s_drug_code"] = greatgreatgrandchild.attrib["code"]
                                        if "codeSystemName" in greatgreatgrandchild.attrib:
                                            source_med_dict["s_drug_code_type"] = greatgreatgrandchild.attrib["codeSystemName"]
                                        if "codeSystem" in greatgreatgrandchild.attrib:
                                            source_med_dict["s_drug_code_type_oid"] = greatgreatgrandchild.attrib["codeSystem"]
                                        if "displayName" in greatgreatgrandchild.attrib:
                                            source_med_dict["s_drug_text"] = greatgreatgrandchild.attrib["displayName"]

        result_list += [source_med_dict]

    return result_list


def extract_immunization_source_medication_ccda(xml_doc):
    # Immunizations
    # /ClinicalDocument/component/structuredBody/component/section/code[@code="11369-6"][@codeSystem="2.16.840.1.113883.6.1"]/../entry/substanceAdministration

    source_medication_obj = ps.SourceMedicationObject()


def extract_vitals_source_result_ccda(xml_doc):
    # Vitals
    # ./ClinicalDocument/component/structuredBody/component/section/code[@code="8716-3"][@codeSystem="2.16.840.1.113883.6.1"]/../entry/organizer/component/observation

    source_result_obj = ps.SourceResultObject()


def extract_vitals_source_result_apple_cda(xml_doc):
    # Vitals (Apple CDA)
    # /ClinicalDocument/entry/organizer/code[@code="46680005"][@codeSystem="2.16.840.1.113883.6.96"]/../component/observation

    source_result_obj = ps.SourceResultObject()


def extract_social_history_source_condition(xml_doc):
    # Social history
    # /ClinicalDocument/component/structuredBody/component/section/code[@code="29762-2"][@codeSystem="2.16.840.1.113883.6.1"]/../entry/observation
    pass


def extract_source_note_ccda(xml_doc):
    # Notes
    # /ClinicalDocument/component/structuredBody/component/section/entry/act/code[@code="34109-9"][@codeSystem="2.16.840.1.113883.6.1"]/..
    pass


# Assessment and plan
# /ClinicalDocument/component/structuredBody/component/section/code[@code="51847-2"][@codeSystem="2.16.840.1.113883.6.1"]/..


def generate_patient_identifier(directory, salt):
    """Salt and hash the directory for serving as a patient identifier"""
    
    to_be_hashed = salt + directory
    hashing = hashlib.blake2b(digest_size=16)
    hashing.update(to_be_hashed.encode("utf8"))
    return hashing.hexdigest()


def create_directory(directory):
    if os.path.exists(directory):
        pass
    else:
        print(f"Creating: '{directory}'")
        os.mkdir(directory)


def write_csv_list_dict(file_name, list_dict):
    header = list(list_dict[0].keys())

    with open(file_name, mode="w", newline="") as fw:
        dw = csv.DictWriter(fw, fieldnames=header)
        dw.writeheader()

        for row in list_dict:
            dw.writerow(row)

def parse_xml_file(xml_file_name):
    """Parse the cda xml document"""
    cda = et.parse(xml_file_name)

    return cda


def main(directory, salting):

    p_directory = pathlib.Path(directory)
    search_pattern = str(p_directory) + os.path.sep + "*.xml"

    xml_files_to_process = glob.glob(search_pattern)

    # Setup directories
    output_directory_root = p_directory / "output"
    create_directory(output_directory_root)

    ps_frag_directory = output_directory_root / "ps_frags"
    create_directory(ps_frag_directory)

    ps_directory = output_directory_root / "ps"
    create_directory(ps_directory)

    s_person_id = generate_patient_identifier(directory, salt=salting)

    for xml_file in xml_files_to_process:
        try:
            print(f"Parsing: '{xml_file}'")
            xml_obj = parse_xml_file(xml_file)
        except IOError:
            raise IOError

        # Get patient information
        person_result_list = extract_source_person_ccda(xml_obj, s_person_id, xml_file)

        just_xml_file_name = os.path.split(xml_file)[-1]
        source_person_file_name = "source_person." + just_xml_file_name + ".csv"

        source_person_path = ps_frag_directory / source_person_file_name
        print(f"Writing: '{source_person_path}'")
        write_csv_list_dict(source_person_path, person_result_list)

        source_result_lab_file_name = "source_result.lab." + just_xml_file_name + ".csv"
        lab_result_list = extract_labs_source_result_ccda(xml_obj, s_person_id, xml_file)
        source_result_lab_path = ps_frag_directory / source_result_lab_file_name
        print(f"Writing '{source_result_lab_path}")
        write_csv_list_dict(source_result_lab_path, lab_result_list)

if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser(description="Convert C-CDA XML & Apple Healthkit CDA XML files "
                                                        "to the prepared source format for conversion to OHDSI cdm")

    arg_parse_obj.add_argument("-d", "--directory", dest="directory", default="./test/samples/patient_1/")
    arg_parse_obj.add_argument("--salt", dest="salt", default="Mighty salty today")

    arg_obj = arg_parse_obj.parse_args()

    main(arg_obj.directory, arg_obj.salt)

