import json
import xml.etree.ElementTree as et
import hashlib
import os
import pathlib
import csv
import json
import preparedsource2ohdsi.prepared_source as ps
import argparse


CDANS = "{urn:hl7-org:v3}"

def expand_tag(tag_name):
    return f"{CDANS}{tag_name}"

ext = expand_tag

# cda = et.parse(cda_filename)
# observations = list(cda.iterfind("./{urn:hl7-org:v3}entry/{urn:hl7-org:v3}organizer/{urn:hl7-org:v3}component/{urn:hl7-org:v3}observation"))
def extract_source_person_ccda(xml_doc):
    """Extract details for source_person from the C-CDA the patientRole section"""
    # source_person
    # ./ClinicalDocument/recordTarget/patientRole/patient/birthTime
    # ./ClinicalDocument/recordTarget/patientRole/patient/administrativeGenderCode
    # ./ClinicalDocument/recordTarget/patientRole/patient/raceCode
    # ./ClinicalDocument/recordTarget/patientRole/patient/ethnicGroupCode
    # ./ClinicalDocument/recordTarget/patientRole/patient/sdtc:deceasedInd
    # ./ClinicalDocument/recordTarget/patientRole/patient/sdtc:deceasedTime

    source_person_obj = ps.SourcePersonObject()


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

    root = xml_doc.getroot()
    find_labs_xpath = './/{urn:hl7-org:v3}structuredBody/{urn:hl7-org:v3}component/{urn:hl7-org:v3}section/{urn:hl7-org:v3}code[@code="30954-2"][@codeSystem="2.16.840.1.113883.6.1"]/../{urn:hl7-org:v3}entry/{urn:hl7-org:v3}organizer/{urn:hl7-org:v3}component/{urn:hl7-org:v3}observation/'

    source_result_obj = ps.SourceResultObject()

    result_list = []
    for element in root.iterfind(find_labs_xpath):
        source_result_dict = source_result_obj.dict_template()
        source_result_dict["s_person_id"] = source_person_id
        source_result_dict["s_source_system"] = f"c-cda/{source_cda_file_name}"

        result_list += [source_result_dict]

    return result_list


def extract_source_medication_ccda(xml_doc, source_person_id, source_cda_file_name):
    """Extract medications from the medications section of C-CDA document"""
    # Medications
    # /ClinicalDocument/component/structuredBody/component/section/code[@code="10160-0"][@codeSystem="2.16.840.1.113883.6.1"]/../entry/substanceAdministration
    # /{urn:hl7-org:v3}structuredBody/{urn:hl7-org:v3}component/{urn:hl7-org:v3}section/{urn:hl7-org:v3}code[@code="10160-0"][@codeSystem="2.16.840.1.113883.6.1"]/..

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
                        if "value" in grandchild.attrib: # TODO: Add date formater
                            source_med_dict["s_start_medication_datetime"] = grandchild.attrib["value"]
                    elif grandchild.tag == ext("high"):
                        if "value" in grandchild.attrib:
                            source_med_dict["s_end_medication_datetime"] = grandchild.attrib["value"]
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


def parse_xml_file(xml_file_name):
    """Parse the cda xml document"""
    cda = et.parse(xml_file_name)

    return cda

