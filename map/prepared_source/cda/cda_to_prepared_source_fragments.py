import xml.etree.ElementTree as et
import hashlib
import os
import pathlib
import csv
import json
import preparedsource2ohdsi.prepared_source as ps
import argparse
import glob
import datetime
import base64
import pypdf

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

    if "s_text" not in code_dict:
        code_dict["s_text"] = None

    return code_dict


def expand_tag(tag_name):
    return f"{CDANS}{tag_name}"


ext = expand_tag


def clean_datetime(datetime_str):
    # TODO: Implement consistent formatting
    if len(datetime_str) > 8 and ('+' in datetime_str or "-" in datetime_str):
        if ".000" in datetime_str:
            datetime_str = datetime_str.replace(".000", "")
        return datetime.datetime.strptime(datetime_str, "%Y%m%d%H%M%S%z")
    elif len(datetime_str) == 14:
        return datetime.datetime.strptime(datetime_str, "%Y%m%d%H%M%S")
    elif len(datetime_str) == 8:
        return datetime.datetime.strptime(datetime_str, "%Y%m%d")
    else:
        return datetime_str


def clean_file_name(file_name):

    directory, file_name = os.path.split(os.path.abspath(file_name))

    parent_directory = directory.split(os.path.sep)[-1]
    return parent_directory + "/" + file_name

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
    source_person_dict["s_source_system"] = f"c-cda/{clean_file_name(source_cda_file_name)}"

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


def extract_problems_source_condition_ccda(xml_doc, source_person_id, source_cda_file_name, snomed_code="55607006"):
    # Active problem lists
    # /ClinicalDocument/component/structuredBody/component/section/entry/act/entryRelationship/observation/code[@code="404684003"][@codeSystem="2.16.840.1.113883.6.96"]/..
    source_condition_obj = ps.SourceConditionObject()

    # 11450-4
    find_problems_xpath = './/{urn:hl7-org:v3}structuredBody/{urn:hl7-org:v3}component/{urn:hl7-org:v3}section/{urn:hl7-org:v3}entry/{urn:hl7-org:v3}act/{urn:hl7-org:v3}entryRelationship/{urn:hl7-org:v3}observation/{urn:hl7-org:v3}code[@code="' + snomed_code + '"][@codeSystem="2.16.840.1.113883.6.96"]/..'

    root = xml_doc.getroot()

    source_condition_obj = ps.SourceConditionObject()

    result_list = []
    for element in root.iterfind(find_problems_xpath):
        source_prob_dict = source_condition_obj.dict_template()
        source_prob_dict["s_person_id"] = source_person_id
        source_prob_dict["s_source_system"] = f"c-cda/{clean_file_name(source_cda_file_name)}"

        for child in element:
            if child.tag == ext("id"):
                if "root" in child.attrib:
                    source_prob_dict["s_id"] = child.attrib["root"]
            elif child.tag == ext("effectiveTime"):

                for grandchild in child:
                    if grandchild.tag == ext("low"):
                        if "value" in grandchild.attrib:
                            source_prob_dict["s_start_condition_datetime"] = clean_datetime(
                                grandchild.attrib["value"])
                    elif grandchild.tag == ext("high"):
                        if "value" in grandchild.attrib:
                            source_prob_dict["s_end_condition_datetime"] = clean_datetime(
                                grandchild.attrib["value"])

            elif child.tag == ext("value"):
                code_dict = code_to_dict(child)
                source_prob_dict["s_condition_code"] = code_dict["s_code"]
                source_prob_dict["s_condition_code_type"] = code_dict["s_code_type"]
                source_prob_dict["s_condition_code_type_oid"] = code_dict["s_code_type_oid"]

                if "displayName" in child.attrib:
                    source_prob_dict["s_condition"] = child.attrib["displayName"]

            elif child.tag == ext("statusCode"):
                if "code" in child.attrib:
                    source_prob_dict["s_status"] = child.attrib["code"]

            source_prob_dict["m_condition_type_code"] = "OMOP4976890"
            source_prob_dict["m_condition_type_code_type"] = "OHDSI"
            source_prob_dict["m_condition_type_code_type_oid"] = "ohdsi.type_concept"

        result_list += [source_prob_dict]
    return result_list


def extract_source_procedures_ccda(xml_doc, source_person_id, source_cda_file_name):
    # Procedures
    # /ClinicalDocument/component/structuredBody/component/section/code[@code="47519-4"][@codeSystem="2.16.840.1.113883.6.1"]/../entry/observation
    # /ClinicalDocument/component/structuredBody/component/section/code[@code="47519-4"][@codeSystem="2.16.840.1.113883.6.1"]/../entry/procedure

    find_procedures_xpath = './/{urn:hl7-org:v3}structuredBody/{urn:hl7-org:v3}component/{urn:hl7-org:v3}section/{urn:hl7-org:v3}code[@code="47519-4"][@codeSystem="2.16.840.1.113883.6.1"]/../{urn:hl7-org:v3}entry/{urn:hl7-org:v3}procedure'
    root = xml_doc.getroot()

    source_procedure_obj = ps.SourceProcedureObject()

    result_list = []
    for element in root.iterfind(find_procedures_xpath):
        if element.tag == ext("procedure"):
            source_proc_dict = source_procedure_obj.dict_template()
            source_proc_dict["s_person_id"] = source_person_id
            source_proc_dict["s_source_system"] = f"c-cda/{clean_file_name(source_cda_file_name)}"

            for child in element:

                if child.tag == ext("id"):
                    if "root" in child.attrib:
                        source_proc_dict["s_id"] = child.attrib["root"]

                elif child.tag == ext("effectiveTime"):

                    if "value" in child.attrib:
                        source_proc_dict["s_start_procedure_datetime"] = clean_datetime(child.attrib["value"])

                elif child.tag == ext("code"):
                    code_dict = code_to_dict(child)
                    source_proc_dict["s_procedure_code"] = code_dict["s_code"]
                    source_proc_dict["s_procedure_code_type"] = code_dict["s_code_type"]
                    source_proc_dict["s_procedure_code_type_oid"] = code_dict["s_code_type_oid"]

            result_list += [source_proc_dict]

    return result_list


FIND_ENCOUNTERS_XPATH = './/{urn:hl7-org:v3}structuredBody/{urn:hl7-org:v3}component/{urn:hl7-org:v3}section/{urn:hl7-org:v3}code[@code="46240-8"][@codeSystem="2.16.840.1.113883.6.1"]/../{urn:hl7-org:v3}entry/{urn:hl7-org:v3}encounter'

# HL7 ActEncounterCode (2.16.840.1.113883.5.4) -> (m_visit_type, m_visit_type_code), same "ohdsi.visit"
# domain codes used by the FHIR converter's classify_visit_type() and by map/prepared_source/synthea's
# visit_type.csv, so a C-CDA-derived and a FHIR-derived encounter classify the same way.
ACT_ENCOUNTER_CODE_TO_VISIT = {
    "AMB": ("Outpatient Visit", "OP"),
    "SS": ("Outpatient Visit", "OP"),
    "EMER": ("Emergency Room Visit", "ER"),
    "IMP": ("Inpatient Visit", "IP"),
    "ACUTE": ("Inpatient Visit", "IP"),
    "NONAC": ("Non-hospital institution Visit", "LTCP"),
    "VR": ("Telehealth", "OMOP5556618"),
    "HH": ("Home Visit", "OMOP4822459"),
    "FLD": ("Home Visit", "OMOP4822459"),
}


def classify_visit_type_ccda(act_encounter_code, text):
    """Best-effort (m_visit_type, m_visit_type_code): a direct ActEncounterCode lookup when a real
    coded value is present, else a keyword heuristic over whatever free text (originalText/displayName)
    the encounter's code element carried -- mirrors the FHIR converter's classify_visit_type()."""
    if act_encounter_code:
        mapped = ACT_ENCOUNTER_CODE_TO_VISIT.get(act_encounter_code)
        if mapped:
            return mapped
    combined = (text or "").lower()
    if "emergency" in combined:
        return "Emergency Room Visit", "ER"
    if "inpatient" in combined and "outpatient" not in combined:
        return "Inpatient Visit", "IP"
    return "Outpatient Visit", "OP"


def _encounter_id_tuple(element):
    """(root, extension) for the first well-formed <id> child; None if there isn't one. An <encounter>
    can carry more than one <id> (e.g. a nullFlavor="UNK" placeholder alongside a real MSK-EncounterId
    one) -- every <id> child must be checked, not just the first, and the 2 placeholder-only files'
    <encounter><id nullFlavor="NA"/></encounter> stubs (with no other <id>) correctly yield None."""
    for child in element:
        if child.tag == ext("id") and "nullFlavor" not in child.attrib and "root" in child.attrib:
            return child.attrib["root"], child.attrib.get("extension")
    return None


def _first_participant_location_role(element):
    for child in element:
        if child.tag == ext("participant") and child.attrib.get("typeCode") == "LOC":
            for role in child:
                if role.tag == ext("participantRole"):
                    return role
    return None


def _location_name_and_address(participant_role):
    """Returns (facility_name, address_dict) from a participantRole -- a C-CDA participantRole bundles
    facility name (playingEntity/name) and address (addr) in one element, unlike FHIR's separate
    Location/Organization resources."""
    name = None
    address = {}
    for child in participant_role:
        if child.tag == ext("playingEntity"):
            for grandchild in child:
                if grandchild.tag == ext("name") and grandchild.text:
                    name = grandchild.text.strip() or None
        elif child.tag == ext("addr"):
            for grandchild in child:
                tag = grandchild.tag.replace(CDANS, "")
                if tag == "streetAddressLine" and "s_address_1" not in address and grandchild.text:
                    address["s_address_1"] = grandchild.text
                elif tag == "city" and grandchild.text:
                    address["s_city"] = grandchild.text
                elif tag == "state" and grandchild.text:
                    address["s_state"] = grandchild.text
                elif tag == "postalCode" and grandchild.text:
                    address["s_zip"] = grandchild.text
                elif tag == "country" and grandchild.text:
                    address["s_country"] = grandchild.text
    return name, address


def _first_performer_assigned_entity(element):
    for child in element:
        if child.tag == ext("performer"):
            for assigned_entity in child:
                if assigned_entity.tag == ext("assignedEntity"):
                    return assigned_entity
    return None


def _provider_key_and_name(assigned_entity):
    """(k_provider, provider_name, npi). Keys on NPI when assignedEntity/id carries one (root
    2.16.840.1.113883.4.6, not nullFlavor); else on the assignedPerson's name; else None (no usable
    identity -- seen for several assignedEntity/id nullFlavor="NI" performers in this corpus)."""
    npi = None
    for child in assigned_entity:
        if child.tag == ext("id") and child.attrib.get("root") == "2.16.840.1.113883.4.6" \
                and "nullFlavor" not in child.attrib:
            npi = child.attrib.get("extension")

    given = family = None
    for child in assigned_entity:
        if child.tag == ext("assignedPerson"):
            for name_el in child:
                if name_el.tag == ext("name"):
                    for part in name_el:
                        if part.tag == ext("given") and given is None and part.text:
                            given = part.text.strip()
                        elif part.tag == ext("family") and family is None and part.text:
                            family = part.text.strip()

    provider_name = " ".join(p for p in (given, family) if p) or None

    if npi:
        return f"c-cda:provider:npi:{npi}", provider_name, npi
    if given or family:
        return f"c-cda:provider:name:{given or ''}_{family or ''}", provider_name, None
    return None, None, None


def _encompassing_encounter_map(root):
    """{(root, extension): <encompassingEncounter> element} for same-file enrichment. Every
    encompassingEncounter observed in this corpus duplicates an id already in the Encounters section
    -- it never introduces a new encounter -- but it does carry dischargeDispositionCode and typed
    (ADM/ATND/REF) encounterParticipant that the Encounters-section entry itself often lacks."""
    result = {}
    find_xpath = f'.//{ext("componentOf")}/{ext("encompassingEncounter")}'
    for encompassing in root.iterfind(find_xpath):
        for child in encompassing:
            if child.tag == ext("id") and "root" in child.attrib:
                result[(child.attrib["root"], child.attrib.get("extension"))] = encompassing
    return result


def _enrich_encounter_from_encompassing(source_encounter_dict, encounter_id, encompassing_map):
    encompassing = encompassing_map.get(encounter_id)
    if encompassing is None:
        return
    attending_provider_key = None
    for child in encompassing:
        if child.tag == ext("dischargeDispositionCode"):
            code_dict = code_to_dict(child)
            text = code_dict["s_text"] or child.attrib.get("displayName")
            if code_dict["s_code"] or text:
                source_encounter_dict["s_discharge_to"] = text
                source_encounter_dict["s_discharge_to_code"] = code_dict["s_code"]
                source_encounter_dict["s_discharge_to_code_type"] = code_dict["s_code_type"]
                source_encounter_dict["s_discharge_to_code_type_oid"] = code_dict["s_code_type_oid"]
        elif child.tag == ext("encounterParticipant") and child.attrib.get("typeCode") == "ATND" \
                and attending_provider_key is None:
            # Only the first ATND participant -- a document can list several (e.g. attending
            # physician and attending nurse both typed ATND); the first is the more authoritative one.
            for assigned_entity in child:
                if assigned_entity.tag == ext("assignedEntity"):
                    key, _name, _npi = _provider_key_and_name(assigned_entity)
                    if key:
                        attending_provider_key = key
    if attending_provider_key:
        source_encounter_dict["k_provider"] = attending_provider_key


def extract_source_encounter_ccda(xml_doc, source_person_id, source_cda_file_name):
    root = xml_doc.getroot()
    encompassing_map = _encompassing_encounter_map(root)
    source_encounter_obj = ps.SourceEncounterObject()

    result_list = []
    for element in root.iterfind(FIND_ENCOUNTERS_XPATH):
        encounter_id = _encounter_id_tuple(element)
        if encounter_id is None:
            continue

        d = source_encounter_obj.dict_template()
        d["s_encounter_id"] = f"c-cda:encounter:{encounter_id[0]}:{encounter_id[1]}"
        d["s_id"] = d["s_encounter_id"]
        d["s_person_id"] = source_person_id
        d["s_source_system"] = f"c-cda/{clean_file_name(source_cda_file_name)}"

        act_encounter_code = None
        visit_text = None
        for child in element:
            if child.tag == ext("effectiveTime"):
                if "value" in child.attrib:
                    dt = clean_datetime(child.attrib["value"])
                    d["s_visit_start_datetime"] = dt
                    d["s_visit_end_datetime"] = dt
                else:
                    for grandchild in child:
                        if grandchild.tag == ext("low") and "value" in grandchild.attrib:
                            d["s_visit_start_datetime"] = clean_datetime(grandchild.attrib["value"])
                        elif grandchild.tag == ext("high") and "value" in grandchild.attrib:
                            d["s_visit_end_datetime"] = clean_datetime(grandchild.attrib["value"])

            elif child.tag == ext("code"):
                code_dict = code_to_dict(child)
                visit_text = code_dict["s_text"] or child.attrib.get("displayName")
                if "nullFlavor" not in child.attrib:
                    d["s_visit_type"] = visit_text
                    d["s_visit_type_code"] = code_dict["s_code"]
                    d["s_visit_type_code_type"] = code_dict["s_code_type"]
                    d["s_visit_type_code_type_oid"] = code_dict["s_code_type_oid"]
                    # Try the code value itself against the ActEncounterCode table regardless of the
                    # reported codeSystem OID: at least one source system in this corpus labels a
                    # real "AMB" ActEncounterCode with the CPT OID instead of 2.16.840.1.113883.5.4 --
                    # these short mnemonic tokens (AMB/EMER/IMP/...) are distinctive enough that a
                    # false-positive match against an unrelated vocabulary is effectively impossible.
                    act_encounter_code = code_dict["s_code"]
                else:
                    d["s_visit_type"] = visit_text

        d["m_visit_type"], d["m_visit_type_code"] = classify_visit_type_ccda(act_encounter_code, visit_text)
        d["m_visit_type_code_type"] = "Visit"
        d["m_visit_type_code_type_oid"] = "ohdsi.visit"
        d["m_visit_source"] = "EHR Encounter Record"
        d["m_visit_source_code"] = "OMOP4976900"
        d["m_visit_source_code_type"] = "Type"
        d["m_visit_source_code_type_oid"] = "ohdsi.type_concept"

        participant_role = _first_participant_location_role(element)
        if participant_role is not None:
            facility_name, _address = _location_name_and_address(participant_role)
            if facility_name:
                d["k_care_site"] = f"c-cda:care_site:{facility_name}"

        assigned_entity = _first_performer_assigned_entity(element)
        if assigned_entity is not None:
            provider_key, _name, _npi = _provider_key_and_name(assigned_entity)
            if provider_key:
                d["k_provider"] = provider_key

        _enrich_encounter_from_encompassing(d, encounter_id, encompassing_map)

        result_list.append(d)
    return result_list


def extract_source_care_site_ccda(xml_doc, source_cda_file_name):
    root = xml_doc.getroot()
    care_sites = {}
    for element in root.iterfind(FIND_ENCOUNTERS_XPATH):
        if _encounter_id_tuple(element) is None:
            continue
        participant_role = _first_participant_location_role(element)
        if participant_role is None:
            continue
        facility_name, address = _location_name_and_address(participant_role)
        if not facility_name:
            continue
        key = f"c-cda:care_site:{facility_name}"
        if key in care_sites:
            continue
        d = ps.SourceCareSiteObject().dict_template()
        d["k_care_site"] = key
        d["s_care_site_name"] = facility_name
        if address:
            d["k_location"] = f"c-cda:location:{facility_name}"
        d["s_source_system"] = f"c-cda/{clean_file_name(source_cda_file_name)}"
        care_sites[key] = d
    return list(care_sites.values())


def extract_source_location_ccda(xml_doc, source_cda_file_name):
    root = xml_doc.getroot()
    locations = {}
    for element in root.iterfind(FIND_ENCOUNTERS_XPATH):
        if _encounter_id_tuple(element) is None:
            continue
        participant_role = _first_participant_location_role(element)
        if participant_role is None:
            continue
        facility_name, address = _location_name_and_address(participant_role)
        if not facility_name or not address:
            continue
        key = f"c-cda:location:{facility_name}"
        if key in locations:
            continue
        d = ps.SourceLocationObject().dict_template()
        d["k_location"] = key
        d["s_location_name"] = facility_name
        d.update(address)
        d["s_source_system"] = f"c-cda/{clean_file_name(source_cda_file_name)}"
        locations[key] = d
    return list(locations.values())


def extract_source_provider_ccda(xml_doc, source_cda_file_name):
    """Provider info from each encounter's own performer/assignedEntity -- NOT from
    documentationOf/serviceEvent/performer, which is document-scoped (spans the whole document's date
    range, often a "StatedByPatient/NOPCP" placeholder) rather than tied to a specific visit."""
    root = xml_doc.getroot()
    providers = {}
    for element in root.iterfind(FIND_ENCOUNTERS_XPATH):
        if _encounter_id_tuple(element) is None:
            continue
        assigned_entity = _first_performer_assigned_entity(element)
        if assigned_entity is None:
            continue
        key, name, npi = _provider_key_and_name(assigned_entity)
        if not key or key in providers:
            continue
        d = ps.SourceProviderObject().dict_template()
        d["k_provider"] = key
        d["s_provider_name"] = name
        d["s_npi"] = npi
        d["s_source_system"] = f"c-cda/{clean_file_name(source_cda_file_name)}"
        providers[key] = d
    return list(providers.values())


def extract_labs_source_result_ccda(xml_doc, source_person_id, source_cda_file_name):
    # Labs
    # /ClinicalDocument/component/structuredBody/component/section/code[@code="30954-2"][@codeSystem="2.16.840.1.113883.6.1"]/../entry/organizer/component/observation
    # .//{urn:hl7-org:v3}structuredBody/{urn:hl7-org:v3}component/{urn:hl7-org:v3}section/{urn:hl7-org:v3}code[@code="30954-2"][@codeSystem="2.16.840.1.113883.6.1"]/../{urn:hl7-org:v3}entry/{urn:hl7-org:v3}organizer/{urn:hl7-org:v3}component/{urn:hl7-org:v3}observation/
    find_labs_xpath = './/{urn:hl7-org:v3}structuredBody/{urn:hl7-org:v3}component/{urn:hl7-org:v3}section/{urn:hl7-org:v3}code[@code="30954-2"][@codeSystem="2.16.840.1.113883.6.1"]/../{urn:hl7-org:v3}entry/{urn:hl7-org:v3}organizer/{urn:hl7-org:v3}component/{urn:hl7-org:v3}observation/.'
    root = xml_doc.getroot()

    source_result_obj = ps.SourceResultObject()

    result_list = []
    for element in root.iterfind(find_labs_xpath):

        source_result_dict = source_result_obj.dict_template()
        source_result_dict["s_person_id"] = source_person_id
        source_result_dict["s_source_system"] = f"c-cda/{clean_file_name(source_cda_file_name)}"

        if element.tag == ext("observation"):
            for child in element:
                if child.tag == ext("id"):
                    if "root" in child.attrib:
                        source_result_dict["s_id"] = child.attrib["root"]

                elif child.tag == ext("effectiveTime"):

                    if "value" in child.attrib:
                        source_result_dict["s_obtained_datetime"] = clean_datetime(child.attrib["value"])

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
                                source_result_dict["s_result_numeric"] = child.attrib["value"]

                            if "unit" in child.attrib:
                                source_result_dict["s_result_unit"] = child.attrib["unit"]
                                if len(source_result_dict["s_result_unit"]):
                                    source_result_dict["s_result_unit_code"] = source_result_dict["s_result_unit"]
                                    source_result_dict["s_result_unit_code_type"] = "UCUM"
                                    source_result_dict["s_result_unit_code_type_oid"] = "2.16.840.1.113883.6.8"

                        elif value_type == "ST":
                            if "value" in child.attrib:
                                source_result_dict["m_result_text"] = child.attrib["value"]
                            else:
                                source_result_dict["m_result_text"] = child.text

                elif child.tag == ext("referenceRange"):
                    for grandchild in child:
                        if grandchild.tag == ext("observationRange"):
                            for greatgrandchild in grandchild:

                                if greatgrandchild.tag == ext("value"):
                                    for greatgreatgrandchild in greatgrandchild:
                                        if greatgreatgrandchild.tag == ext("low"):
                                            if "value" in greatgreatgrandchild.attrib:
                                                source_result_dict["s_result_numeric_lower"] = greatgreatgrandchild.attrib["value"]
                                        elif greatgreatgrandchild.tag == ext("high"):
                                            if "value" in greatgreatgrandchild.attrib:
                                                source_result_dict["s_result_numeric_upper"] = greatgreatgrandchild.attrib["value"]

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
        source_med_dict["s_source_system"] = f"c-cda/{clean_file_name(source_cda_file_name)}"

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

    #TODO: Write this part


def extract_vitals_source_result_ccda(xml_doc, source_person_id, source_cda_file_name):
    # Vitals
    # ./ClinicalDocument/component/structuredBody/component/section/code[@code="8716-3"][@codeSystem="2.16.840.1.113883.6.1"]/../entry/organizer/component/observation

    root = xml_doc.getroot()
    find_labs_xpath = './/{urn:hl7-org:v3}structuredBody/{urn:hl7-org:v3}component/{urn:hl7-org:v3}section/{urn:hl7-org:v3}code[@code="8716-3"][@codeSystem="2.16.840.1.113883.6.1"]/../{urn:hl7-org:v3}entry/{urn:hl7-org:v3}organizer/{urn:hl7-org:v3}component/{urn:hl7-org:v3}observation/.'

    source_result_obj = ps.SourceResultObject()

    result_list = []
    for element in root.iterfind(find_labs_xpath):
        source_result_dict = source_result_obj.dict_template()
        source_result_dict["s_person_id"] = source_person_id
        source_result_dict["s_source_system"] = f"c-cda/{clean_file_name(source_cda_file_name)}"

        for child in element:
            if child.tag == ext("id"):
                if "root" in child.attrib:
                    source_result_dict["s_id"] = child.attrib["root"]

            elif child.tag == ext("effectiveTime"):
                if "value" in child.attrib:
                    source_result_dict["s_obtained_datetime"] = clean_datetime(child.attrib["value"])

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

            elif child.tag == ext("value"):
                if "{http://www.w3.org/2001/XMLSchema-instance}type" in child.attrib:
                    value_type = child.attrib["{http://www.w3.org/2001/XMLSchema-instance}type"]

                    if value_type == "PQ":

                        if "value" in child.attrib:
                            source_result_dict["s_result_numeric"] = child.attrib["value"]

                        if "unit" in child.attrib:
                            source_result_dict["s_result_unit"] = child.attrib["unit"]
                            if len(source_result_dict["s_result_unit"]):
                                source_result_dict["s_result_unit_code"] = source_result_dict["s_result_unit"]
                                source_result_dict["s_result_unit_code_type"] = "UCUM"
                                source_result_dict["s_result_unit_code_type_oid"] = "2.16.840.1.113883.6.8"

        result_list += [source_result_dict]
    return result_list


def extract_source_result_apple_cda(xml_doc, source_person_id, source_cda_file_name, snomed_code="46680005"):
    # Vitals (Apple CDA)
    # /ClinicalDocument/entry/organizer/code[@code="46680005"][@codeSystem="2.16.840.1.113883.6.96"]/../component/observation
    find_vitals_xpath = './/{urn:hl7-org:v3}entry/{urn:hl7-org:v3}organizer/./{urn:hl7-org:v3}code[@code="'+ snomed_code  +'"][@codeSystem="2.16.840.1.113883.6.96"]/.././{urn:hl7-org:v3}component/./{urn:hl7-org:v3}observation'
    root = xml_doc.getroot()
    source_result_obj = ps.SourceResultObject()
    result_list = []
    for element in root.iterfind(find_vitals_xpath):

        source_result_dict = source_result_obj.dict_template()
        source_result_dict["s_person_id"] = source_person_id
        source_result_dict["s_source_system"] = f"cda/{clean_file_name(source_cda_file_name)}"

        for child in element:
            if child.tag == ext("id"):
                if "root" in child.attrib:
                    source_result_dict["s_id"] = child.attrib["root"]

            elif child.tag == ext("effectiveTime"):
                for grandchild in child:
                    if grandchild.tag == ext("low"):
                        if "value" in grandchild.attrib:
                            source_result_dict["s_obtained_datetime"] = clean_datetime(grandchild.attrib["value"])

            elif child.tag == ext("code"):
                code_dict = code_to_dict(child)
                source_result_dict["s_code"] = code_dict["s_code"]
                source_result_dict["s_code_type"] = code_dict["s_code_type"]
                source_result_dict["s_code_type_oid"] = code_dict["s_code_type_oid"]

                if "displayName" in child.attrib:
                    source_result_dict["s_text"] = child.attrib["displayName"]

            elif child.tag == ext("value"):
                if "{http://www.w3.org/2001/XMLSchema-instance}type" in child.attrib:
                    value_type = child.attrib["{http://www.w3.org/2001/XMLSchema-instance}type"]

                    if value_type == "PQ":

                        if "value" in child.attrib:
                            source_result_dict["s_result_numeric"] = child.attrib["value"]

                        if "unit" in child.attrib:
                            source_result_dict["s_result_unit"] = child.attrib["unit"]
                            if len(source_result_dict["s_result_unit"]):
                                source_result_dict["s_result_unit_code"] = source_result_dict["s_result_unit"]
                                source_result_dict["s_result_unit_code_type"] = "UCUM"
                                source_result_dict["s_result_unit_code_type_oid"] = "2.16.840.1.113883.6.8"

            elif child.tag == ext("interpretationCode"):

                code_dict = code_to_dict(child)
                source_result_dict["s_result_code"] = code_dict["s_code"]
                source_result_dict["s_result_code_type"] = code_dict["s_code_type"]
                source_result_dict["s_result_code_type_oid"] = code_dict["s_code_type_oid"]

        result_list += [source_result_dict]

    return result_list


def extract_social_history_source_condition(xml_doc):
    # Social history
    # /ClinicalDocument/component/structuredBody/component/section/code[@code="29762-2"][@codeSystem="2.16.840.1.113883.6.1"]/../entry/observation
    pass


def extract_source_note_ccda(xml_doc, source_person_id, source_cda_file_name):
    # Notes
    # /ClinicalDocument/component/structuredBody/component/section/entry/act/code[@code="34109-9"][@codeSystem="2.16.840.1.113883.6.1"]/..
    find_notes_xpath = './/{urn:hl7-org:v3}structuredBody/{urn:hl7-org:v3}component/{urn:hl7-org:v3}section/{urn:hl7-org:v3}entry/{urn:hl7-org:v3}act/{urn:hl7-org:v3}code[@code="34109-9"][@codeSystem="2.16.840.1.113883.6.1"]/..'
    root = xml_doc.getroot()
    source_note_list = []
    source_note_obj = ps.SourceNoteObject()
    i = 0

    reference_dict = {}
    for element in root.iterfind(find_notes_xpath):
        source_note_dict = source_note_obj.dict_template()
        source_note_dict["s_person_id"] = source_person_id
        source_note_dict["s_source_system"] = f"cda/{clean_file_name(source_cda_file_name)}"

        source_note_dict["s_id"] = f"{i}_{source_note_dict['s_source_system']}"

        source_note_dict["s_note_type"] = "EHR"
        source_note_dict["s_note_type_code_type_oid"] = "ohdsi.type_concept"
        source_note_dict["s_note_type_code"] = "OMOP4976890"
        source_note_dict["m_binary_file_name"] = None

        for child in element:

            if child.tag == ext("code"):
                for grandchild in child:
                    if grandchild.tag == ext("translation"):
                        code_dict = code_to_dict(grandchild)

                        if "displayName" in grandchild.attrib:
                            source_note_dict["s_note_class"] = grandchild.attrib["displayName"]

                        source_note_dict["s_note_class_code"] = code_dict["s_code"]
                        source_note_dict["s_note_class_code_type_oid"] = code_dict["s_code_type_oid"]
                        source_note_dict["s_note_class_code_type"] = code_dict["s_code_type"]

                        source_note_dict["m_note_class_code"] = source_note_dict["s_note_class_code"]
                        source_note_dict["m_note_class_code_type_oid"] = source_note_dict["s_note_class_code_type_oid"]

            elif child.tag == ext("effectiveTime"):
                if "value" in child.attrib:
                    source_note_dict["s_note_datetime"] = clean_datetime(child.attrib["value"])

            elif child.tag == ext("text"): # Need to rework this
                if "representation" in child.attrib:
                    if child.attrib["representation"] == "B64":

                        note_b64 = child.text.strip() #[1:-1]

                        directory, file_name = os.path.split(source_cda_file_name)

                        files_directory = os.path.join(directory, "output", "files")
                        if not os.path.exists(files_directory):
                            os.mkdir(files_directory)

                        binary_file_name = os.path.join(directory, "output", "files", str(i) + "_" + file_name + ".pdf")
                        print(f"Writing '{binary_file_name}'")
                        source_note_dict["m_binary_file_name"] = clean_file_name(binary_file_name)

                        with open(binary_file_name, "wb") as fw:
                            fw.write(base64.standard_b64decode(note_b64))

                        print(f"Extracting text from '{binary_file_name}'")

                        with open(binary_file_name, "rb") as fb:
                            empty_file = False
                            try:
                                pdf_reader = pypdf.PdfReader(fb)
                            except pypdf.errors.EmptyFileError:
                                empty_file = True
                                print(f"Empty file '{binary_file_name}'")

                            p_text = ""

                            if not empty_file:
                                for page_number in range(len(pdf_reader.pages)):
                                    page = pdf_reader.pages[page_number]
                                    extracted_page_text = page.extract_text()

                                    extracted_page_text = extracted_page_text.replace("\xa0", " ")
                                    extracted_page_text += "\n\n"

                                    p_text += extracted_page_text

                            source_note_dict["s_note_text"] = p_text #TODO: deal with conversion issues p_text

                else:
                    for grandchild in child:
                        if grandchild.tag == ext("reference"):
                            if "value" in grandchild.attrib:
                                reference_dict[i] = grandchild.attrib["value"][1:]
        i += 1
        source_note_list += [source_note_dict]

    if len(reference_dict):
        root_obj = xml_doc.getroot()

        for reference in reference_dict:
            content_reference_xpath = f'.//{ext("content")}[@ID="{reference_dict[reference]}"]'
            note_text = ""
            for match in root_obj.iterfind(content_reference_xpath):
                for mtext in match.itertext():
                   note_text += mtext + "\n"

            paragraph_reference_xpath = f'.//{ext("paragraph")}[@ID="{reference_dict[reference]}"]'
            for match in root_obj.iterfind(paragraph_reference_xpath):
                for mtext in match.itertext():
                    note_text += mtext + "\n"

            source_note_list[reference]["s_note_text"] = note_text
                

    return source_note_list

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

    with open(file_name, mode="w", newline="", errors="replace", encoding="utf-8") as fw:
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

    s_generation_dict = {"s_person_id": s_person_id,
                         "prepared_source": {},
                         "fragments": {
                             "source_person": [], "source_result": [], "source_medication": [],
                             "source_condition": [], "source_procedure": [], "source_note": [],
                             "source_encounter": [], "source_care_site": [], "source_location": [],
                             "source_provider": []}
                         }

    # Deduped across ALL files, not per-file: many single-encounter documents in a corpus like this
    # reference the exact same real-world facility/provider (e.g. 11 separate Stony Brook documents
    # all naming "Stony Brook University Hospital"). If each file wrote its own duplicate
    # source_care_site/source_provider row under the same k_care_site/k_provider key, the OHDSI
    # mapper's literal-string-equality joins would fan out one encounter into N duplicate
    # visit_occurrence rows (N = how many duplicate care_site/provider rows shared that key).
    all_care_sites = {}
    all_locations = {}
    all_providers = {}

    for xml_file in xml_files_to_process:
        try:
            print(f"Parsing: '{xml_file}'")
            xml_obj = parse_xml_file(xml_file)
        except IOError:
            raise IOError

        # source_person
        person_result_list = extract_source_person_ccda(xml_obj, s_person_id, xml_file)

        just_xml_file_name = os.path.split(xml_file)[-1]
        source_person_file_name = "source_person." + just_xml_file_name + ".csv"

        source_person_path = ps_frag_directory / source_person_file_name
        print(f"Writing: '{source_person_path}'")

        write_csv_list_dict(source_person_path, person_result_list)
        s_generation_dict["fragments"]["source_person"] += [str(source_person_path.absolute())]

        # Source note
        source_note_file_name = "source_note." + just_xml_file_name + ".csv"
        source_note_list = extract_source_note_ccda(xml_obj, s_person_id, xml_file)
        source_note_path = ps_frag_directory / source_note_file_name
        if len(source_note_list):
            print(f"Writing {len(source_note_list)} rows in  '{source_note_path}")
            write_csv_list_dict(source_note_path, source_note_list)
            s_generation_dict["fragments"]["source_note"] += [str(source_note_path.absolute())]

        # Labs: source_result
        source_result_lab_file_name = "source_result.lab." + just_xml_file_name + ".csv"
        lab_result_list = extract_labs_source_result_ccda(xml_obj, s_person_id, xml_file)
        source_result_lab_path = ps_frag_directory / source_result_lab_file_name

        if len(lab_result_list):
            print(f"Writing {len(lab_result_list)} rows in  '{source_result_lab_path}")
            write_csv_list_dict(source_result_lab_path, lab_result_list)
            s_generation_dict["fragments"]["source_result"] += [str(source_result_lab_path.absolute())]
        else:

            lab_result_list = extract_source_result_apple_cda(xml_obj, s_person_id, xml_file, snomed_code="386053000")
            if len(lab_result_list):
                print(f"Writing {len(lab_result_list)} rows in  '{source_result_lab_path}")
                write_csv_list_dict(source_result_lab_path, lab_result_list)
                s_generation_dict["fragments"]["source_result"] += [str(source_result_lab_path.absolute())]
            else:
                print(f"Did not find c-cda lab results; skipping: '{source_result_lab_path}'")

        # source_medication
        source_medication_file_name = "source_medication." + just_xml_file_name + ".csv"
        source_medication_list = extract_source_medication_ccda(xml_obj, s_person_id, xml_file)

        source_medication_path = ps_frag_directory / source_medication_file_name
        if len(source_medication_list):
            print(f"Writing {len(source_medication_list)} rows in '{source_medication_path}'")
            write_csv_list_dict(source_medication_path, source_medication_list)
            s_generation_dict["fragments"]["source_medication"] += [str(source_medication_path.absolute())]

        else:
            print(f"Did not find c-cda coded medications; skipping: '{source_medication_path}'")


        # Problems: source_condition
        source_condition_file_name = "source_condition." + just_xml_file_name + ".csv"
        source_condition_list = extract_problems_source_condition_ccda(xml_obj, s_person_id, xml_file)

        if len(source_condition_list) == 0:
            source_condition_list = extract_problems_source_condition_ccda(xml_obj, s_person_id, xml_file,
                                                                           snomed_code="404684003")

        source_condition_path = ps_frag_directory / source_condition_file_name
        if len(source_condition_list):
            print(f"Writing {len(source_condition_list)} rows in '{source_condition_path}'")
            write_csv_list_dict(source_condition_path, source_condition_list)
            s_generation_dict["fragments"]["source_condition"] += [str(source_condition_path.absolute())]

        else:
            print(f"Did not find c-cda coded problems; skipping: '{source_condition_path}'")

        # Procedure
        source_procedure_file_name = "source_procedure." + just_xml_file_name + ".csv"
        source_procedure_path = ps_frag_directory / source_procedure_file_name
        source_procedure_list = extract_source_procedures_ccda(xml_obj, s_person_id, xml_file)

        if len(source_procedure_list):
            print(f"Writing {len(source_procedure_list)} rows in '{source_procedure_path}'")
            write_csv_list_dict(source_procedure_path, source_procedure_list)
            s_generation_dict["fragments"]["source_procedure"] += [str(source_procedure_path.absolute())]
        else:
            print(f"Did not find c-cda coded procedure; "
                  f"skipping: '{source_procedure_path}'")
        # Vitals: source_result
        source_result_vital_file_name = "source_result.vital." + just_xml_file_name + ".csv"
        vital_result_list = extract_vitals_source_result_ccda(xml_obj, s_person_id, xml_file)
        source_result_vital_path = ps_frag_directory / source_result_vital_file_name

        if len(vital_result_list):
            print(f"Writing {len(vital_result_list)} rows in  '{source_result_vital_path}")
            write_csv_list_dict(source_result_vital_path, vital_result_list)
            s_generation_dict["fragments"]["source_result"] += [str(source_result_vital_path.absolute())]
        else:

            # Vitals Apple CDA
            source_result_vital_apple_file_name = "source_result.vital.apple." + just_xml_file_name + ".csv"
            vital_result_list = extract_source_result_apple_cda(xml_obj, s_person_id, xml_file)
            source_result_vital_apple_path = ps_frag_directory / source_result_vital_apple_file_name

            if len(vital_result_list):
                print(f"Writing {len(vital_result_list)} rows in  '{source_result_vital_apple_path}")
                write_csv_list_dict(source_result_vital_apple_path, vital_result_list)
                s_generation_dict["fragments"]["source_result"] += [str(source_result_vital_apple_path.absolute())]
            else:
                print(f"Did not find cda vital results; skipping: '{source_result_vital_apple_path}'")

        # Encounters, and the care_site/location/provider rows they reference
        source_encounter_file_name = "source_encounter." + just_xml_file_name + ".csv"
        source_encounter_path = ps_frag_directory / source_encounter_file_name
        source_encounter_list = extract_source_encounter_ccda(xml_obj, s_person_id, xml_file)

        if len(source_encounter_list):
            print(f"Writing {len(source_encounter_list)} rows in '{source_encounter_path}'")
            write_csv_list_dict(source_encounter_path, source_encounter_list)
            s_generation_dict["fragments"]["source_encounter"] += [str(source_encounter_path.absolute())]
        else:
            print(f"Did not find c-cda encounters; skipping: '{source_encounter_path}'")

        for row in extract_source_care_site_ccda(xml_obj, xml_file):
            all_care_sites.setdefault(row["k_care_site"], row)

        for row in extract_source_location_ccda(xml_obj, xml_file):
            all_locations.setdefault(row["k_location"], row)

        for row in extract_source_provider_ccda(xml_obj, xml_file):
            all_providers.setdefault(row["k_provider"], row)

    if all_care_sites:
        source_care_site_path = ps_frag_directory / "source_care_site.csv"
        print(f"Writing {len(all_care_sites)} rows in '{source_care_site_path}'")
        write_csv_list_dict(source_care_site_path, list(all_care_sites.values()))
        s_generation_dict["fragments"]["source_care_site"] += [str(source_care_site_path.absolute())]

    if all_locations:
        source_location_path = ps_frag_directory / "source_location.csv"
        print(f"Writing {len(all_locations)} rows in '{source_location_path}'")
        write_csv_list_dict(source_location_path, list(all_locations.values()))
        s_generation_dict["fragments"]["source_location"] += [str(source_location_path.absolute())]

    if all_providers:
        source_provider_path = ps_frag_directory / "source_provider.csv"
        print(f"Writing {len(all_providers)} rows in '{source_provider_path}'")
        write_csv_list_dict(source_provider_path, list(all_providers.values()))
        s_generation_dict["fragments"]["source_provider"] += [str(source_provider_path.absolute())]

    json_file_path = output_directory_root / "s_files_generated.json"
    with open(json_file_path, "w") as fw:
        json.dump(s_generation_dict, fw, indent=3)


if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser(description="Convert C-CDA XML & Apple Healthkit CDA XML files "
                                                        "to the prepared source format for conversion to OHDSI cdm")

    arg_parse_obj.add_argument("-d", "--directory", dest="directory", default="./test/samples/patient_1/")
    arg_parse_obj.add_argument("--salt", dest="salt", default="Mighty salty today")

    arg_obj = arg_parse_obj.parse_args()

    main(arg_obj.directory, arg_obj.salt)

