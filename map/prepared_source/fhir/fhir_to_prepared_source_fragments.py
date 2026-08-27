import argparse
import base64
import csv
import json
import os
import pathlib
import re
import zipfile
from html.parser import HTMLParser

import preparedsource2ohdsi.prepared_source as ps

# Map FHIR coding.system URLs to the bare OIDs used elsewhere in this repo (map/ohdsi/mappings/vocabulary_to_oid.json)
FHIR_SYSTEM_TO_OID = {
    "http://loinc.org": "2.16.840.1.113883.6.1",
    "http://snomed.info/sct": "2.16.840.1.113883.6.96",
    "http://www.nlm.nih.gov/research/umls/rxnorm": "2.16.840.1.113883.6.88",
    "http://hl7.org/fhir/sid/cvx": "2.16.840.1.113883.12.292",
    "http://unitsofmeasure.org": "2.16.840.1.113883.6.8",
    "http://www.ama-assn.org/go/cpt": "2.16.840.1.113883.6.12",
    "http://hl7.org/fhir/sid/icd-10-cm": "2.16.840.1.113883.6.90",
}

# Same OID the C-CDA converter uses for administrativeGenderCode (HL7 AdministrativeGender)
ADMINISTRATIVE_GENDER_OID = "2.16.840.1.113883.5.1"

# Minimum decoded byte length for a DocumentReference/Binary attachment to be treated as a real
# note rather than an empty placeholder stub (Epic emits ~48 byte "<html></html>"-style stubs for
# DocumentReferences that only exist to point at a DiagnosticReport/Observation, not real note text)
MIN_NOTE_BYTES = 100

DEFAULT_CODE_CROSSWALK_PATH = os.path.join(os.path.dirname(__file__), "mappings", "epic_local_code_crosswalk.csv")


def load_code_crosswalk(csv_path=DEFAULT_CODE_CROSSWALK_PATH):
    """Load the Epic-local-code -> LOINC/SNOMED crosswalk (map/prepared_source/fhir/mappings/
    epic_local_code_crosswalk.csv). Many Observation/Procedure codes in this Epic export carry only
    an Epic-internal component/procedure id, with no LOINC/SNOMED alternative on the resource itself
    -- this hand-maintained CSV fills in a standard code for the ones we've identified so far. Keyed
    by (s_code_type_oid, s_code); extend the CSV as new unmapped codes turn up in future exports.

    Returns {} (i.e. no crosswalking) if the file doesn't exist, so callers don't need a special case."""
    if not os.path.exists(csv_path):
        return {}
    crosswalk = {}
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            crosswalk[(row["s_code_type_oid"], row["s_code"])] = {
                "m_code": row["m_code"],
                "m_code_type": row["m_code_type"],
                "m_code_type_oid": row["m_code_type_oid"],
            }
    return crosswalk


def strip_oid_prefix(system):
    if system is None:
        return None
    if system.startswith("urn:oid:"):
        return system[len("urn:oid:"):]
    return system


def pick_coding(coding_list, preferred_systems=()):
    """Pick the first coding whose system is in preferred_systems (priority order); else the first coding."""
    if not coding_list:
        return None
    for system in preferred_systems:
        for coding in coding_list:
            if coding.get("system") == system:
                return coding
    return coding_list[0]


def coding_to_code_dict(coding):
    """Turn a FHIR coding into s_code/s_code_type/s_code_type_oid, normalizing well-known systems to their OID."""
    if coding is None:
        return {"s_code": None, "s_code_type": None, "s_code_type_oid": None}
    system = coding.get("system")
    oid = FHIR_SYSTEM_TO_OID.get(system, strip_oid_prefix(system))
    return {
        "s_code": coding.get("code"),
        "s_code_type": coding.get("display") or system,
        "s_code_type_oid": oid,
    }


def ref_id(reference):
    """Extract the resource id from a FHIR 'ResourceType/id' reference string."""
    if not reference:
        return None
    return reference.split("/")[-1]


def index_by_id(records):
    return {r["id"]: r for r in records if "id" in r}


def resource_key(filename):
    """Turn 'FHIR\\allergyintolerance163.NDJSON' into 'allergyintolerance'."""
    base = filename.replace("\\", "/").split("/")[-1]
    if base.upper().endswith(".NDJSON"):
        base = base[:-len(".NDJSON")]
    match = re.match(r"^([A-Za-z]+)\d*$", base)
    return (match.group(1) if match else base).lower()


def build_resource_index(zf):
    """Map resource_key -> zip entry name, for every FHIR/*.NDJSON entry in the export zip."""
    index = {}
    for name in zf.namelist():
        normalized = name.replace("\\", "/")
        if normalized.startswith("FHIR/") and normalized.upper().endswith(".NDJSON"):
            index[resource_key(name)] = name
    return index


def load_zip_ndjson(zf, filename):
    data = zf.read(filename).decode("utf-8", errors="replace")
    records = []
    for line in data.splitlines():
        line = line.strip()
        if line:
            records.append(json.loads(line))
    return records


class _HTMLTextExtractor(HTMLParser):
    """Minimal, dependency-free HTML-to-text stripper for note bodies."""

    def __init__(self):
        super().__init__()
        self._chunks = []

    def handle_data(self, data):
        self._chunks.append(data)

    def handle_starttag(self, tag, attrs):
        if tag in ("br", "p", "div", "tr", "li"):
            self._chunks.append("\n")

    def get_text(self):
        return "".join(self._chunks)


def html_to_text(html_bytes):
    extractor = _HTMLTextExtractor()
    extractor.feed(html_bytes.decode("utf-8", errors="replace"))
    return extractor.get_text()


def clean_file_name(file_name):
    directory, name = os.path.split(os.path.abspath(file_name))
    parent_directory = directory.split(os.path.sep)[-1]
    return parent_directory + "/" + name


def create_directory(directory):
    if not os.path.exists(directory):
        print(f"Creating: '{directory}'")
        os.makedirs(directory)


def write_csv_list_dict(file_name, list_dict):
    header = list(list_dict[0].keys())
    with open(file_name, mode="w", newline="", errors="replace", encoding="utf-8") as fw:
        dw = csv.DictWriter(fw, fieldnames=header)
        dw.writeheader()
        for row in list_dict:
            dw.writerow(row)


# --- extractors, one per prepared_source table -------------------------------------------------

def extract_source_person_fhir(patient, source_person_id, source_system):
    d = ps.SourcePersonObject().dict_template()
    d["s_person_id"] = source_person_id
    d["s_id"] = patient.get("id")
    d["s_source_system"] = source_system

    gender = patient.get("gender")
    d["s_gender"] = gender
    birthsex = None
    for extension in patient.get("extension", []):
        if extension.get("url") == "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex":
            birthsex = extension.get("valueCode")
    d["s_gender_code"] = birthsex or {"male": "M", "female": "F"}.get(gender)
    d["s_gender_code_type"] = "AdministrativeGender"
    d["s_gender_code_type_oid"] = ADMINISTRATIVE_GENDER_OID

    d["s_birth_datetime"] = patient.get("birthDate")
    if patient.get("deceasedDateTime"):
        d["s_death_datetime"] = patient.get("deceasedDateTime")

    for extension in patient.get("extension", []):
        url = extension.get("url")
        if url == "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race":
            for sub in extension.get("extension", []):
                if sub.get("url") == "ombCategory":
                    coding = sub.get("valueCoding", {})
                    code_dict = coding_to_code_dict(coding)
                    d["s_race"] = coding.get("display")
                    d["s_race_code"] = code_dict["s_code"]
                    d["s_race_code_type"] = "CDC Race and Ethnicity"
                    d["s_race_code_type_oid"] = code_dict["s_code_type_oid"]
        elif url == "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity":
            for sub in extension.get("extension", []):
                if sub.get("url") == "ombCategory":
                    coding = sub.get("valueCoding", {})
                    code_dict = coding_to_code_dict(coding)
                    d["s_ethnicity"] = coding.get("display")
                    d["s_ethnicity_code"] = code_dict["s_code"]
                    d["s_ethnicity_code_type"] = "CDC Race and Ethnicity"
                    d["s_ethnicity_code_type_oid"] = code_dict["s_code_type_oid"]

    return [d]


def classify_visit_type(encounter):
    """Best-effort keyword classification of an Encounter into OP/IP/ER.

    Epic's Encounter.class here is a uniform, uninformative administrative wrapper
    ("Support OP Encounter") for every encounter in this export, so the real signal
    is in Encounter.type[].text / type[].coding[].display instead.
    """
    texts = [encounter.get("class", {}).get("display", "")]
    for type_entry in encounter.get("type", []):
        texts.append(type_entry.get("text", ""))
        for coding in type_entry.get("coding", []):
            texts.append(coding.get("display", ""))
    combined = " ".join(t for t in texts if t).lower()

    if "emergency" in combined:
        return "Emergency Room Visit", "ER"
    if "inpatient" in combined and "outpatient" not in combined:
        return "Inpatient Visit", "IP"
    return "Outpatient Visit", "OP"


def extract_source_encounter_fhir(encounters, source_person_id, source_system):
    rows = []
    for encounter in encounters:
        d = ps.SourceEncounterObject().dict_template()
        d["s_encounter_id"] = f"fhir:Encounter/{encounter['id']}"
        d["s_person_id"] = source_person_id
        d["s_id"] = d["s_encounter_id"]
        d["s_source_system"] = source_system

        period = encounter.get("period", {})
        d["s_visit_start_datetime"] = period.get("start")
        d["s_visit_end_datetime"] = period.get("end") or period.get("start")

        type_entries = encounter.get("type", [])
        if type_entries:
            d["s_visit_type"] = type_entries[0].get("text")
            codings = type_entries[0].get("coding", [])
            if codings:
                d["s_visit_type_code"] = codings[0].get("code")
                d["s_visit_type_code_type"] = codings[0].get("display")
        else:
            d["s_visit_type"] = encounter.get("class", {}).get("display")

        m_visit_type, m_visit_type_code = classify_visit_type(encounter)
        d["m_visit_type"] = m_visit_type
        d["m_visit_type_code"] = m_visit_type_code
        d["m_visit_type_code_type"] = "Visit"
        d["m_visit_type_code_type_oid"] = "ohdsi.visit"

        d["m_visit_source"] = "EHR Encounter Record"
        d["m_visit_source_code"] = "OMOP4976900"
        d["m_visit_source_code_type"] = "Type"
        d["m_visit_source_code_type_oid"] = "ohdsi.type_concept"

        service_provider_ref = encounter.get("serviceProvider", {}).get("reference")
        if service_provider_ref:
            d["k_care_site"] = f"fhir:Organization/{ref_id(service_provider_ref)}"

        provider_ref = None
        participants = encounter.get("participant", [])
        for participant in participants:
            for type_coding in participant.get("type", [{}])[0].get("coding", []) if participant.get("type") else []:
                if type_coding.get("code") == "ADM":
                    provider_ref = participant.get("individual", {}).get("reference")
        if provider_ref is None:
            for participant in participants:
                individual_ref = participant.get("individual", {}).get("reference")
                if individual_ref and individual_ref.startswith("Practitioner/"):
                    provider_ref = individual_ref
                    break
        if provider_ref:
            d["k_provider"] = f"fhir:Practitioner/{ref_id(provider_ref)}"

        rows.append(d)
    return rows


def extract_source_location_fhir(locations_by_id, referenced_location_ids, source_system):
    rows = []
    for location_id in sorted(referenced_location_ids):
        location = locations_by_id.get(location_id)
        if location is None:
            continue
        d = ps.SourceLocationObject().dict_template()
        d["k_location"] = f"fhir:Location/{location_id}"
        d["s_source_system"] = source_system
        d["s_location_name"] = location.get("name")
        address = location.get("address", {})
        d["s_address_1"] = "; ".join(address.get("line", [])) or None
        d["s_city"] = address.get("city")
        d["s_state"] = address.get("state")
        d["s_zip"] = address.get("postalCode")
        d["s_country"] = address.get("country")
        rows.append(d)
    return rows


def extract_source_care_site_fhir(organizations_by_id, referenced_org_ids, source_system):
    rows = []
    for org_id in sorted(referenced_org_ids):
        organization = organizations_by_id.get(org_id)
        if organization is None:
            continue
        d = ps.SourceCareSiteObject().dict_template()
        d["k_care_site"] = f"fhir:Organization/{org_id}"
        d["s_care_site_name"] = organization.get("name")
        d["s_source_system"] = source_system
        rows.append(d)
    return rows


def extract_source_provider_fhir(practitioners_by_id, referenced_practitioner_ids, source_system):
    rows = []
    for practitioner_id in sorted(referenced_practitioner_ids):
        practitioner = practitioners_by_id.get(practitioner_id)
        if practitioner is None:
            continue
        d = ps.SourceProviderObject().dict_template()
        d["k_provider"] = f"fhir:Practitioner/{practitioner_id}"
        d["s_source_system"] = source_system

        names = practitioner.get("name", [])
        if names:
            d["s_provider_name"] = names[0].get("text")

        for identifier in practitioner.get("identifier", []):
            if identifier.get("system") in ("http://hl7.org/fhir/sid/us-npi", "urn:oid:2.16.840.1.113883.4.6"):
                d["s_npi"] = identifier.get("value")
                break

        qualifications = practitioner.get("qualification", [])
        if qualifications:
            d["s_specialty"] = qualifications[0].get("code", {}).get("text")

        rows.append(d)
    return rows


def extract_source_result_fhir(observations, source_person_id, source_system, code_crosswalk=None):
    code_crosswalk = code_crosswalk or {}
    rows = []
    for observation in observations:
        d = ps.SourceResultObject().dict_template()
        d["s_person_id"] = source_person_id
        d["s_id"] = f"fhir:Observation/{observation['id']}"
        d["s_source_system"] = source_system

        coding = pick_coding(observation.get("code", {}).get("coding", []), ["http://loinc.org"])
        code_dict = coding_to_code_dict(coding)
        d["s_code"] = code_dict["s_code"]
        d["s_code_type"] = code_dict["s_code_type"]
        d["s_code_type_oid"] = code_dict["s_code_type_oid"]
        d["s_name"] = observation.get("code", {}).get("text")

        crosswalked = code_crosswalk.get((d["s_code_type_oid"], d["s_code"]))
        if crosswalked:
            d["m_code"] = crosswalked["m_code"]
            d["m_code_type"] = crosswalked["m_code_type"]
            d["m_code_type_oid"] = crosswalked["m_code_type_oid"]

        d["s_obtained_datetime"] = observation.get("effectiveDateTime")

        value_quantity = observation.get("valueQuantity")
        if value_quantity:
            d["s_result_numeric"] = value_quantity.get("value")
            unit = value_quantity.get("unit")
            d["s_result_unit"] = unit
            if unit:
                d["s_result_unit_code"] = value_quantity.get("code") or unit
                d["s_result_unit_code_type"] = "UCUM"
                d["s_result_unit_code_type_oid"] = "2.16.840.1.113883.6.8"
        elif "valueString" in observation:
            d["m_result_text"] = observation["valueString"]
        elif "valueCodeableConcept" in observation:
            value_coding = pick_coding(observation["valueCodeableConcept"].get("coding", []),
                                       ["http://loinc.org", "http://snomed.info/sct"])
            d["m_result_text"] = (value_coding or {}).get("display") or observation["valueCodeableConcept"].get("text")

        reference_ranges = observation.get("referenceRange", [])
        if reference_ranges:
            low = reference_ranges[0].get("low", {}).get("value")
            high = reference_ranges[0].get("high", {}).get("value")
            if low is not None:
                d["s_result_numeric_lower"] = low
            if high is not None:
                d["s_result_numeric_upper"] = high

        encounter_ref = observation.get("encounter", {}).get("reference")
        if encounter_ref:
            d["s_encounter_id"] = f"fhir:Encounter/{ref_id(encounter_ref)}"

        for performer in observation.get("performer", []):
            performer_ref = performer.get("reference")
            if performer_ref and performer_ref.startswith("Practitioner/"):
                d["k_provider"] = f"fhir:Practitioner/{ref_id(performer_ref)}"
                break

        rows.append(d)
    return rows


def _procedure_performer_ref(procedure):
    asserter_ref = procedure.get("asserter", {}).get("reference")
    if asserter_ref:
        return asserter_ref
    for performer in procedure.get("performer", []):
        actor_ref = performer.get("actor", {}).get("reference")
        if actor_ref:
            return actor_ref
    return None


def extract_source_procedure_fhir(procedures, source_person_id, source_system, code_crosswalk=None):
    code_crosswalk = code_crosswalk or {}
    rows = []
    for procedure in procedures:
        d = ps.SourceProcedureObject().dict_template()
        d["s_person_id"] = source_person_id
        d["s_id"] = f"fhir:Procedure/{procedure['id']}"
        d["s_source_system"] = source_system

        coding = pick_coding(procedure.get("code", {}).get("coding", []),
                             ["http://snomed.info/sct", "http://www.ama-assn.org/go/cpt"])
        code_dict = coding_to_code_dict(coding)
        d["s_procedure_code"] = code_dict["s_code"]
        d["s_procedure_code_type"] = code_dict["s_code_type"]
        d["s_procedure_code_type_oid"] = code_dict["s_code_type_oid"]

        # prepared_source.py's SourceProcedureObject has no separate m_procedure_code field (unlike
        # source_result/source_person/etc), so a crosswalk hit replaces the source code fields
        # outright rather than sitting alongside them. Full traceability back to the original Epic
        # code is still available via s_id (the FHIR Procedure resource id) and the crosswalk CSV
        # itself, keyed by the original (s_code_type_oid, s_code).
        crosswalked = code_crosswalk.get((d["s_procedure_code_type_oid"], d["s_procedure_code"]))
        if crosswalked:
            d["s_procedure_code"] = crosswalked["m_code"]
            d["s_procedure_code_type"] = crosswalked["m_code_type"]
            d["s_procedure_code_type_oid"] = crosswalked["m_code_type_oid"]

        d["s_start_procedure_datetime"] = procedure.get("performedDateTime") or \
            procedure.get("performedPeriod", {}).get("start")

        encounter_ref = procedure.get("encounter", {}).get("reference")
        if encounter_ref:
            d["s_encounter_id"] = f"fhir:Encounter/{ref_id(encounter_ref)}"

        performer_ref = _procedure_performer_ref(procedure)
        if performer_ref and performer_ref.startswith("Practitioner/"):
            d["k_provider"] = f"fhir:Practitioner/{ref_id(performer_ref)}"

        rows.append(d)
    return rows


def extract_source_immunizations_as_procedure_fhir(immunizations, source_person_id, source_system):
    """Map Immunization -> source_procedure (CVX-coded); prepared_source.py has no dedicated
    immunization table today, and a one-time vaccine administration fits the Procedure domain
    better than Medication (dose/route/drug-text fields don't apply)."""
    rows = []
    for immunization in immunizations:
        d = ps.SourceProcedureObject().dict_template()
        d["s_person_id"] = source_person_id
        d["s_id"] = f"fhir:Immunization/{immunization['id']}"
        d["s_source_system"] = f"{source_system}/immunization"

        coding = pick_coding(immunization.get("vaccineCode", {}).get("coding", []),
                             ["http://hl7.org/fhir/sid/cvx"])
        code_dict = coding_to_code_dict(coding)
        d["s_procedure_code"] = code_dict["s_code"]
        d["s_procedure_code_type"] = "CVX"
        d["s_procedure_code_type_oid"] = code_dict["s_code_type_oid"]

        d["s_start_procedure_datetime"] = immunization.get("occurrenceDateTime")
        rows.append(d)
    return rows


def extract_source_medication_fhir(medication_requests, medications_by_id, source_person_id, source_system):
    rows = []
    for medication_request in medication_requests:
        d = ps.SourceMedicationObject().dict_template()
        d["s_person_id"] = source_person_id
        d["s_id"] = f"fhir:MedicationRequest/{medication_request['id']}"
        d["s_source_system"] = source_system
        d["s_status"] = medication_request.get("status")

        medication_ref = medication_request.get("medicationReference", {}).get("reference")
        medication = medications_by_id.get(ref_id(medication_ref)) if medication_ref else None
        if medication is not None:
            coding = pick_coding(medication.get("code", {}).get("coding", []),
                                 ["http://www.nlm.nih.gov/research/umls/rxnorm"])
            code_dict = coding_to_code_dict(coding)
            d["s_drug_code"] = code_dict["s_code"]
            d["s_drug_code_type"] = "RxNorm" if coding and coding.get("system") == \
                "http://www.nlm.nih.gov/research/umls/rxnorm" else code_dict["s_code_type"]
            d["s_drug_code_type_oid"] = code_dict["s_code_type_oid"]
            d["s_drug_text"] = medication.get("code", {}).get("text")
        else:
            d["s_drug_text"] = medication_request.get("medicationReference", {}).get("display")

        d["s_start_medication_datetime"] = medication_request.get("authoredOn")

        encounter_ref = medication_request.get("encounter", {}).get("reference")
        if encounter_ref:
            d["s_encounter_id"] = f"fhir:Encounter/{ref_id(encounter_ref)}"

        dosage_instructions = medication_request.get("dosageInstruction", [])
        if dosage_instructions:
            dosage = dosage_instructions[0]
            route_coding = pick_coding(dosage.get("route", {}).get("coding", []), ["http://snomed.info/sct"])
            if route_coding:
                route_code_dict = coding_to_code_dict(route_coding)
                d["s_route"] = route_coding.get("display")
                d["s_route_code"] = route_code_dict["s_code"]
                d["s_route_code_type"] = route_code_dict["s_code_type"]
                d["s_route_code_type_oid"] = route_code_dict["s_code_type_oid"]

            dose_and_rate = dosage.get("doseAndRate", [])
            if dose_and_rate:
                dose_quantity = dose_and_rate[0].get("doseQuantity", {})
                d["s_dose"] = dose_quantity.get("value")
                d["s_dose_unit"] = dose_quantity.get("unit")
                if dose_quantity.get("code"):
                    d["s_dose_unit_code"] = dose_quantity.get("code")
                    d["s_dose_unit_code_type"] = "UCUM"
                    d["s_dose_unit_code_type_oid"] = "2.16.840.1.113883.6.8"

            d["s_patient_instructions"] = dosage.get("patientInstruction")

        requester_ref = medication_request.get("requester", {}).get("reference")
        if requester_ref and requester_ref.startswith("Practitioner/"):
            d["k_provider"] = f"fhir:Practitioner/{ref_id(requester_ref)}"

        rows.append(d)
    return rows


def _pick_note_attachment(content_entries):
    """Prefer the text/html representation of a note over text/rtf (both are present for most
    DocumentReferences here); fall back to whichever attachment comes first."""
    first_attachment = None
    for entry in content_entries:
        attachment = entry.get("attachment", {})
        if first_attachment is None:
            first_attachment = attachment
        if "html" in (attachment.get("contentType") or ""):
            return attachment
    return first_attachment


def extract_source_note_fhir(document_references, binaries_by_id, source_person_id, source_system,
                             files_directory):
    rows = []
    i = 0
    for document_reference in document_references:
        content_entries = document_reference.get("content", [])
        if not content_entries:
            continue

        attachment = _pick_note_attachment(content_entries)
        if attachment is None:
            continue

        binary_id = ref_id(attachment.get("url"))
        binary = binaries_by_id.get(binary_id)
        if binary is None or not binary.get("data"):
            continue

        raw_bytes = base64.standard_b64decode(binary["data"])
        if len(raw_bytes) < MIN_NOTE_BYTES:
            # Epic emits empty placeholder attachments for DocumentReferences that only exist to
            # point at a DiagnosticReport/Observation; skip these rather than creating blank notes.
            continue

        content_type = binary.get("contentType", "")
        extension = "html" if "html" in content_type else ("rtf" if "rtf" in content_type else "bin")

        create_directory(files_directory)
        binary_file_name = os.path.join(files_directory, f"{i}_{binary_id}.{extension}")
        with open(binary_file_name, "wb") as fw:
            fw.write(raw_bytes)

        if extension == "html":
            note_text = html_to_text(raw_bytes)
        else:
            # No RTF parser dependency in this repo; store the raw RTF markup as a lower-fidelity
            # fallback rather than pulling in a new third-party library for a handful of notes.
            note_text = raw_bytes.decode("utf-8", errors="replace")

        d = ps.SourceNoteObject().dict_template()
        d["s_id"] = f"fhir:DocumentReference/{document_reference['id']}"
        d["s_person_id"] = source_person_id
        d["s_source_system"] = source_system
        d["s_note_type"] = "EHR"
        d["s_note_type_code_type_oid"] = "ohdsi.type_concept"
        d["s_note_type_code"] = "OMOP4976890"
        d["m_binary_file_name"] = clean_file_name(binary_file_name)
        d["s_note_text"] = note_text
        d["s_note_datetime"] = document_reference.get("date")

        encounters = document_reference.get("context", {}).get("encounter", [])
        if encounters:
            d["s_encounter_id"] = f"fhir:Encounter/{ref_id(encounters[0].get('reference'))}"

        type_coding = pick_coding(document_reference.get("type", {}).get("coding", []), ["http://loinc.org"])
        if type_coding:
            type_code_dict = coding_to_code_dict(type_coding)
            d["s_note_class"] = type_coding.get("display") or document_reference.get("type", {}).get("text")
            d["s_note_class_code"] = type_code_dict["s_code"]
            d["s_note_class_code_type"] = type_code_dict["s_code_type"]
            d["s_note_class_code_type_oid"] = type_code_dict["s_code_type_oid"]
            d["m_note_class_code"] = d["s_note_class_code"]
            d["m_note_class_code_type_oid"] = d["s_note_class_code_type_oid"]

        for author in document_reference.get("author", []):
            author_ref = author.get("reference")
            if author_ref and author_ref.startswith("Practitioner/"):
                d["k_provider"] = f"fhir:Practitioner/{ref_id(author_ref)}"
                break

        rows.append(d)
        i += 1
    return rows


# AllergyIntolerance rows whose code indicates "no information collected" rather than a real,
# diagnosed allergy (e.g. SNOMED 1631000175102 "Patient not asked") are excluded rather than
# turned into a spurious condition row.
ALLERGY_NO_INFO_SNOMED_CODES = {"1631000175102"}


def extract_source_condition_fhir(allergies, source_person_id, source_system):
    rows = []
    for allergy in allergies:
        d = ps.SourceConditionObject().dict_template()
        d["s_person_id"] = source_person_id
        d["s_id"] = f"fhir:AllergyIntolerance/{allergy['id']}"
        d["s_source_system"] = source_system
        d["m_condition_type_code"] = "OMOP4976890"
        d["m_condition_type_code_type_oid"] = "ohdsi.type_concept"

        coding = pick_coding(allergy.get("code", {}).get("coding", []), ["http://snomed.info/sct"])
        code_dict = coding_to_code_dict(coding)
        d["s_condition_code"] = code_dict["s_code"]
        d["s_condition_code_type"] = code_dict["s_code_type"]
        d["s_condition_code_type_oid"] = code_dict["s_code_type_oid"]
        d["s_condition"] = allergy.get("code", {}).get("text")
        d["s_condition_code_domain"] = "Observation"
        d["s_start_condition_datetime"] = allergy.get("recordedDate") or allergy.get("onsetDateTime")

        clinical_status_codings = allergy.get("clinicalStatus", {}).get("coding", [])
        if clinical_status_codings:
            d["s_status"] = clinical_status_codings[0].get("code")

        if code_dict["s_code"] in ALLERGY_NO_INFO_SNOMED_CODES:
            d["i_exclude"] = 1
            d["i_exclude_reason"] = "No allergy information on file (source asserts patient not asked / no known allergy data)"

        rows.append(d)
    return rows


# --- orchestration -------------------------------------------------------------------------

def collect_referenced_location_ids(encounters):
    ids = set()
    for encounter in encounters:
        for location_entry in encounter.get("location", []):
            location_id = ref_id(location_entry.get("location", {}).get("reference"))
            if location_id:
                ids.add(location_id)
    return ids


def collect_referenced_organization_ids(encounters):
    ids = set()
    for encounter in encounters:
        org_id = ref_id(encounter.get("serviceProvider", {}).get("reference"))
        if org_id:
            ids.add(org_id)
    return ids


def collect_referenced_practitioner_ids(encounters, procedures, medication_requests, document_references):
    ids = set()
    for encounter in encounters:
        for participant in encounter.get("participant", []):
            individual_ref = participant.get("individual", {}).get("reference")
            if individual_ref:
                ids.add(ref_id(individual_ref))
    for procedure in procedures:
        performer_ref = _procedure_performer_ref(procedure)
        if performer_ref and performer_ref.startswith("Practitioner/"):
            ids.add(ref_id(performer_ref))
    for medication_request in medication_requests:
        requester_ref = medication_request.get("requester", {}).get("reference")
        if requester_ref and requester_ref.startswith("Practitioner/"):
            ids.add(ref_id(requester_ref))
    for document_reference in document_references:
        for author in document_reference.get("author", []):
            author_ref = author.get("reference")
            if author_ref and author_ref.startswith("Practitioner/"):
                ids.add(ref_id(author_ref))
    ids.discard(None)
    return ids


def main(directory, zip_path, person_id, code_crosswalk_path=DEFAULT_CODE_CROSSWALK_PATH):
    p_directory = pathlib.Path(directory)

    output_directory_root = p_directory / "output"
    create_directory(output_directory_root)

    ps_frag_directory = output_directory_root / "ps_frags"
    create_directory(ps_frag_directory)

    ps_directory = output_directory_root / "ps"
    create_directory(ps_directory)

    files_directory = str(output_directory_root / "files")

    source_system = f"fhir/{os.path.basename(zip_path)}"

    zf = zipfile.ZipFile(zip_path)
    entries = build_resource_index(zf)

    def load(key):
        name = entries.get(key)
        return load_zip_ndjson(zf, name) if name else []

    patients = load("patient")
    if not patients:
        raise ValueError(f"No Patient resource found in '{zip_path}'")
    patient = patients[0]

    encounters = load("encounter")
    observations = load("observation")
    procedures = load("procedure")
    medication_requests = load("medicationrequest")
    medications = load("medication")
    document_references = load("documentreference")
    binaries = load("binary")
    locations = load("location")
    organizations = load("organization")
    practitioners = load("practitioner")
    allergies = load("allergyintolerance")
    immunizations = load("immunization")

    medications_by_id = index_by_id(medications)
    binaries_by_id = index_by_id(binaries)
    locations_by_id = index_by_id(locations)
    organizations_by_id = index_by_id(organizations)
    practitioners_by_id = index_by_id(practitioners)

    referenced_location_ids = collect_referenced_location_ids(encounters)
    referenced_org_ids = collect_referenced_organization_ids(encounters)
    referenced_practitioner_ids = collect_referenced_practitioner_ids(
        encounters, procedures, medication_requests, document_references)

    code_crosswalk = load_code_crosswalk(code_crosswalk_path)
    print(f"Loaded {len(code_crosswalk)} entries from code crosswalk '{code_crosswalk_path}'")

    s_generation_dict = {"s_person_id": person_id, "prepared_source": {}, "fragments": {}}

    def write_table(table_name, rows):
        if not rows:
            print(f"No rows for '{table_name}'; skipping")
            return
        file_path = ps_frag_directory / f"{table_name}.fhir.csv"
        print(f"Writing {len(rows)} rows in '{file_path}'")
        write_csv_list_dict(str(file_path), rows)
        s_generation_dict["fragments"].setdefault(table_name, [])
        s_generation_dict["fragments"][table_name] += [str(file_path.absolute())]

    write_table("source_person", extract_source_person_fhir(patient, person_id, source_system))
    write_table("source_encounter", extract_source_encounter_fhir(encounters, person_id, source_system))
    write_table("source_location", extract_source_location_fhir(locations_by_id, referenced_location_ids, source_system))
    write_table("source_care_site", extract_source_care_site_fhir(organizations_by_id, referenced_org_ids, source_system))
    write_table("source_provider", extract_source_provider_fhir(practitioners_by_id, referenced_practitioner_ids, source_system))
    write_table("source_result", extract_source_result_fhir(observations, person_id, source_system, code_crosswalk))

    procedure_rows = extract_source_procedure_fhir(procedures, person_id, source_system, code_crosswalk) + \
        extract_source_immunizations_as_procedure_fhir(immunizations, person_id, source_system)
    write_table("source_procedure", procedure_rows)

    write_table("source_medication", extract_source_medication_fhir(medication_requests, medications_by_id, person_id, source_system))
    write_table("source_note", extract_source_note_fhir(document_references, binaries_by_id, person_id, source_system, files_directory))
    write_table("source_condition", extract_source_condition_fhir(allergies, person_id, source_system))

    json_file_path = output_directory_root / "s_files_generated.json"
    with open(json_file_path, "w") as fw:
        json.dump(s_generation_dict, fw, indent=3)
    print(f"Wrote manifest '{json_file_path}'")


if __name__ == "__main__":
    arg_parse_obj = argparse.ArgumentParser(
        description="Convert an Epic FHIR EHI export (NDJSON files inside a zip) to the prepared "
                     "source format for conversion to OHDSI CDM")

    arg_parse_obj.add_argument("-d", "--directory", dest="directory", required=True,
                               help="Output directory root; output/{ps_frags,ps,files} are created under it")
    arg_parse_obj.add_argument("--zip", dest="zip_path", required=True,
                               help="Path to the Epic EHI export zip file (containing a FHIR/ folder of NDJSON files)")
    arg_parse_obj.add_argument("--person-id", dest="person_id", required=True,
                               help="s_person_id to stamp on every row. Must match the s_person_id already "
                                    "used for this patient in other sources (e.g., the value generated by "
                                    "the C-CDA converter's generate_patient_identifier()) or a duplicate "
                                    "OMOP person will be created when combined with those other sources.")
    arg_parse_obj.add_argument("--code-crosswalk", dest="code_crosswalk_path", default=DEFAULT_CODE_CROSSWALK_PATH,
                               help="CSV mapping Epic-local result/procedure codes to LOINC/SNOMED "
                                    "(default: mappings/epic_local_code_crosswalk.csv next to this script). "
                                    "Extend that file as new unmapped Epic-internal codes turn up.")

    arg_obj = arg_parse_obj.parse_args()
    main(arg_obj.directory, arg_obj.zip_path, arg_obj.person_id, arg_obj.code_crosswalk_path)
