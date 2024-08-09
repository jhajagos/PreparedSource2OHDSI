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

# cda = et.parse(cda_filename)
# observations = list(cda.iterfind("./{urn:hl7-org:v3}entry/{urn:hl7-org:v3}organizer/{urn:hl7-org:v3}component/{urn:hl7-org:v3}observation"))

def extract_source_person_ccda(xml_doc):
    # source_person
    # ./ClinicalDocument/recordTarget/patientRole/patient/birthTime
    # ./ClinicalDocument/recordTarget/patientRole/patient/administrativeGenderCode
    # ./ClinicalDocument/recordTarget/patientRole/patient/raceCode
    # ./ClinicalDocument/recordTarget/patientRole/patient/ethnicGroupCode
    # ./ClinicalDocument/recordTarget/patientRole/patient/sdtc:deceasedInd
    # ./ClinicalDocument/recordTarget/patientRole/patient/sdtc:deceasedTime

    pass


def extract_source_provider_ccda(xml_doc):
    # source_provider
    # ./ClinicalDocument/documentationOf/serviceEvent/performer
    # ./ClinicalDocument/component/structuredBody/component/section/code --Get Section

    pass


def extract_problems_source_condition_ccda(xml_doc):
    # Active problem lists
    # /ClinicalDocument/component/structuredBody/component/section/entry/act/entryRelationship/observation/code[@code="404684003"][@codeSystem="2.16.840.1.113883.6.96"]/..
    pass


def extract_source_procedures_ccda(xml_doc):
    # Procedures
    # /ClinicalDocument/component/structuredBody/component/section/code[@code="47519-4"][@codeSystem="2.16.840.1.113883.6.1"]/../entry/observation
    # /ClinicalDocument/component/structuredBody/component/section/code[@code="47519-4"][@codeSystem="2.16.840.1.113883.6.1"]/../entry/procedure
    pass


def extract_source_encounter_ccda(xml_doc):
    # Encounters
    # /ClinicalDocument/component/structuredBody/component/section/code[@code="46240-8"][@codeSystem="2.16.840.1.113883.6.1"]/../entry/encounter
    pass


def extract_source_medication_ccda(xml_doc, source_person_id, source_cda_file_name):
    """Extract medications from the medications section of C-CDA document"""
    # Medications
    # /ClinicalDocument/component/structuredBody/component/section/code[@code="10160-0"][@codeSystem="2.16.840.1.113883.6.1"]/../entry/substanceAdministration
    # /{urn:hl7-org:v3}structuredBody/{urn:hl7-org:v3}component/{urn:hl7-org:v3}section/{urn:hl7-org:v3}code[@code="10160-0"][@codeSystem="2.16.840.1.113883.6.1"]/..

    root = xml_doc.getroot()
    find_meds_xpath = './/{urn:hl7-org:v3}structuredBody/{urn:hl7-org:v3}component/{urn:hl7-org:v3}section/{urn:hl7-org:v3}code[@code="10160-0"][@codeSystem="2.16.840.1.113883.6.1"]/../{urn:hl7-org:v3}entry/{urn:hl7-org:v3}substanceAdministration'

    source_med_obj = ps.SourceMedicationObject()

    result_list = []
    for element in root.iterfind(find_meds_xpath):

        source_med_dict = source_med_obj.dict_template()
        source_med_dict["s_person_id"] = source_person_id

        for child in element:

            print(child.tag)
            print(child.attrib)
            print(child)
            print(dir(child))
            raise RuntimeError

        result_list += [source_med_dict]

    return result_list


def extract_immunization_source_medication_ccda(xml_doc):
    # Immunizations
    # /ClinicalDocument/component/structuredBody/component/section/code[@code="11369-6"][@codeSystem="2.16.840.1.113883.6.1"]/../entry/substanceAdministration
    pass


def extract_labs_source_result_ccda(xml_doc):
    # Labs
    # /ClinicalDocument/component/structuredBody/component/section/code[@code="30954-2"][@codeSystem="2.16.840.1.113883.6.1"]/../entry/organizer/component/observation
    pass


def extract_vitals_source_result_ccda(xml_doc):
    # Vitals
    # ./ClinicalDocument/component/structuredBody/component/section/code[@code="8716-3"][@codeSystem="2.16.840.1.113883.6.1"]/../entry/organizer/component/observation
    pass


def extract_vitals_source_result_apple_cda(xml_doc):
    # Vitals (Apple CDA)
    # /ClinicalDocument/entry/organizer/code[@code="46680005"][@codeSystem="2.16.840.1.113883.6.96"]/../component/observation
    pass


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
    to_be_hashed = salt + directory
    hashing = hashlib.blake2b(digest_size=16)
    hashing.update(to_be_hashed.encode("utf8"))
    return hashing.hexdigest()


def parse_xml_file(xml_file_name):

    cda = et.parse(xml_file_name)

    return cda

