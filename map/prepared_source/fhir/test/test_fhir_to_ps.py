import unittest

from ..fhir_to_prepared_source_fragments import (
    DEFAULT_CODE_CROSSWALK_PATH,
    classify_visit_type,
    coding_to_code_dict,
    extract_source_care_site_fhir,
    extract_source_condition_fhir,
    extract_source_encounter_fhir,
    extract_source_immunizations_as_procedure_fhir,
    extract_source_location_fhir,
    extract_source_medication_fhir,
    extract_source_note_fhir,
    extract_source_person_fhir,
    extract_source_procedure_fhir,
    extract_source_provider_fhir,
    extract_source_result_fhir,
    load_code_crosswalk,
    pick_coding,
    ref_id,
    resource_key,
    strip_oid_prefix,
)

# All ids/names below are synthetic -- do not replace with real export data.
SOURCE_SYSTEM = "fhir/test-export.zip"
PERSON_ID = "test-person-id"

PATIENT = {
    "resourceType": "Patient",
    "id": "pat-1",
    "gender": "female",
    "birthDate": "1980-01-02",
    "extension": [
        {
            "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex",
            "valueCode": "F",
        },
        {
            "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
            "extension": [
                {"url": "ombCategory", "valueCoding": {
                    "system": "urn:oid:2.16.840.1.113883.6.238", "code": "2106-3", "display": "White"}},
            ],
        },
        {
            "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
            "extension": [
                {"url": "ombCategory", "valueCoding": {
                    "system": "urn:oid:2.16.840.1.113883.6.238", "code": "2186-5",
                    "display": "Not Hispanic or Latino"}},
            ],
        },
    ],
}

ENCOUNTER_OUTPATIENT = {
    "id": "enc-1",
    "class": {"display": "Support OP Encounter"},
    "type": [{"text": "Outpatient Encounter", "coding": [{"display": "Outpatient Encounter"}]}],
    "period": {"start": "2022-09-13", "end": "2022-09-13"},
    "serviceProvider": {"reference": "Organization/org-1"},
    "participant": [
        {"type": [{"coding": [{"code": "ADM"}]}], "individual": {"reference": "Practitioner/prac-1"}},
    ],
    "location": [{"location": {"reference": "Location/loc-1"}}],
}

ENCOUNTER_EMERGENCY = {
    "id": "enc-2",
    "class": {"display": "Support OP Encounter"},
    "type": [{"text": "Emergency Room Visit"}],
    "period": {"start": "2023-01-01"},
}


class TestHelpers(unittest.TestCase):
    def test_strip_oid_prefix(self):
        self.assertEqual("2.16.840.1.113883.6.238", strip_oid_prefix("urn:oid:2.16.840.1.113883.6.238"))
        self.assertEqual("http://loinc.org", strip_oid_prefix("http://loinc.org"))
        self.assertIsNone(strip_oid_prefix(None))

    def test_ref_id(self):
        self.assertEqual("abc123", ref_id("Patient/abc123"))
        self.assertIsNone(ref_id(None))
        self.assertIsNone(ref_id(""))

    def test_pick_coding_prefers_listed_system(self):
        codings = [{"system": "urn:oid:1.2.3", "code": "X"}, {"system": "http://loinc.org", "code": "8302-2"}]
        picked = pick_coding(codings, ["http://loinc.org"])
        self.assertEqual("8302-2", picked["code"])

    def test_pick_coding_falls_back_to_first(self):
        codings = [{"system": "urn:oid:1.2.3", "code": "X"}]
        self.assertEqual("X", pick_coding(codings, ["http://loinc.org"])["code"])
        self.assertIsNone(pick_coding([], ["http://loinc.org"]))

    def test_coding_to_code_dict_maps_known_system_to_oid(self):
        d = coding_to_code_dict({"system": "http://loinc.org", "code": "8302-2", "display": "Body height"})
        self.assertEqual("8302-2", d["s_code"])
        self.assertEqual("2.16.840.1.113883.6.1", d["s_code_type_oid"])

    def test_resource_key_strips_trailing_digits_and_extension(self):
        self.assertEqual("patient", resource_key("FHIR\\Patient162.NDJSON"))
        self.assertEqual("allergyintolerance", resource_key("FHIR\\allergyintolerance163.NDJSON"))
        self.assertEqual("medicationrequest", resource_key("FHIR/medicationrequest170.NDJSON"))
        self.assertEqual("medication", resource_key("FHIR/Medication175.NDJSON"))


class TestExtractSourcePerson(unittest.TestCase):
    def test_extract_source_person(self):
        rows = extract_source_person_fhir(PATIENT, PERSON_ID, SOURCE_SYSTEM)
        self.assertEqual(1, len(rows))
        row = rows[0]
        self.assertEqual(PERSON_ID, row["s_person_id"])
        self.assertEqual("F", row["s_gender_code"])
        self.assertEqual("1980-01-02", row["s_birth_datetime"])
        self.assertEqual("2106-3", row["s_race_code"])
        self.assertEqual("2.16.840.1.113883.6.238", row["s_race_code_type_oid"])
        self.assertEqual("2186-5", row["s_ethnicity_code"])


class TestExtractSourceEncounter(unittest.TestCase):
    def test_classify_visit_type_outpatient(self):
        m_visit_type, m_visit_type_code = classify_visit_type(ENCOUNTER_OUTPATIENT)
        self.assertEqual("OP", m_visit_type_code)

    def test_classify_visit_type_emergency(self):
        m_visit_type, m_visit_type_code = classify_visit_type(ENCOUNTER_EMERGENCY)
        self.assertEqual("ER", m_visit_type_code)

    def test_extract_source_encounter(self):
        rows = extract_source_encounter_fhir([ENCOUNTER_OUTPATIENT], PERSON_ID, SOURCE_SYSTEM)
        self.assertEqual(1, len(rows))
        row = rows[0]
        self.assertEqual("fhir:Encounter/enc-1", row["s_encounter_id"])
        self.assertEqual(PERSON_ID, row["s_person_id"])
        self.assertEqual("fhir:Organization/org-1", row["k_care_site"])
        self.assertEqual("fhir:Practitioner/prac-1", row["k_provider"])
        self.assertEqual("OP", row["m_visit_type_code"])
        self.assertEqual("ohdsi.visit", row["m_visit_type_code_type_oid"])


class TestExtractSourceLocationCareSiteProvider(unittest.TestCase):
    def test_extract_source_location_filters_to_referenced(self):
        locations_by_id = {
            "loc-1": {"id": "loc-1", "name": "Referenced Clinic", "address": {"city": "Anytown", "state": "NY"}},
            "loc-2": {"id": "loc-2", "name": "Unreferenced Clinic"},
        }
        rows = extract_source_location_fhir(locations_by_id, {"loc-1"}, SOURCE_SYSTEM)
        self.assertEqual(1, len(rows))
        self.assertEqual("fhir:Location/loc-1", rows[0]["k_location"])
        self.assertEqual("Anytown", rows[0]["s_city"])

    def test_extract_source_care_site_filters_to_referenced(self):
        orgs_by_id = {
            "org-1": {"id": "org-1", "name": "Referenced Org"},
            "org-2": {"id": "org-2", "name": "Data Conversion Vendor"},
        }
        rows = extract_source_care_site_fhir(orgs_by_id, {"org-1"}, SOURCE_SYSTEM)
        self.assertEqual(1, len(rows))
        self.assertEqual("Referenced Org", rows[0]["s_care_site_name"])

    def test_extract_source_provider(self):
        practitioners_by_id = {
            "prac-1": {
                "id": "prac-1",
                "name": [{"text": "Jane Test, MD"}],
                "identifier": [{"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567890"}],
            }
        }
        rows = extract_source_provider_fhir(practitioners_by_id, {"prac-1"}, SOURCE_SYSTEM)
        self.assertEqual(1, len(rows))
        self.assertEqual("Jane Test, MD", rows[0]["s_provider_name"])
        self.assertEqual("1234567890", rows[0]["s_npi"])


class TestExtractSourceResult(unittest.TestCase):
    def test_extract_source_result_quantity(self):
        observation = {
            "id": "obs-1",
            "code": {"coding": [{"system": "http://loinc.org", "code": "8302-2", "display": "Body height"}],
                     "text": "Height"},
            "effectiveDateTime": "2022-09-13T12:18:00Z",
            "valueQuantity": {"value": 179, "unit": "cm", "code": "cm"},
            "encounter": {"reference": "Encounter/enc-1"},
        }
        rows = extract_source_result_fhir([observation], PERSON_ID, SOURCE_SYSTEM)
        self.assertEqual(1, len(rows))
        row = rows[0]
        self.assertEqual("8302-2", row["s_code"])
        self.assertEqual(179, row["s_result_numeric"])
        self.assertEqual("cm", row["s_result_unit"])
        self.assertEqual("fhir:Encounter/enc-1", row["s_encounter_id"])

    def test_extract_source_result_string_value(self):
        observation = {
            "id": "obs-2",
            "code": {"text": "Final Diagnosis"},
            "valueString": "Benign.",
        }
        rows = extract_source_result_fhir([observation], PERSON_ID, SOURCE_SYSTEM)
        self.assertEqual("Benign.", rows[0]["m_result_text"])


class TestExtractSourceProcedure(unittest.TestCase):
    def test_extract_source_procedure(self):
        procedure = {
            "id": "proc-1",
            "code": {"coding": [{"system": "http://snomed.info/sct", "code": "123", "display": "Test procedure"}]},
            "performedDateTime": "2020-01-01T00:00:00Z",
            "encounter": {"reference": "Encounter/enc-1"},
            "asserter": {"reference": "Practitioner/prac-1"},
        }
        rows = extract_source_procedure_fhir([procedure], PERSON_ID, SOURCE_SYSTEM)
        self.assertEqual(1, len(rows))
        self.assertEqual("123", rows[0]["s_procedure_code"])
        self.assertEqual("2.16.840.1.113883.6.96", rows[0]["s_procedure_code_type_oid"])
        self.assertEqual("fhir:Practitioner/prac-1", rows[0]["k_provider"])

    def test_extract_source_immunizations_as_procedure(self):
        immunization = {
            "id": "imm-1",
            "vaccineCode": {"coding": [{"system": "http://hl7.org/fhir/sid/cvx", "code": "208"}]},
            "occurrenceDateTime": "2021-04-03",
        }
        rows = extract_source_immunizations_as_procedure_fhir([immunization], PERSON_ID, SOURCE_SYSTEM)
        self.assertEqual(1, len(rows))
        self.assertEqual("208", rows[0]["s_procedure_code"])
        self.assertEqual("CVX", rows[0]["s_procedure_code_type"])


class TestExtractSourceMedication(unittest.TestCase):
    def test_extract_source_medication(self):
        medication_request = {
            "id": "mr-1",
            "status": "active",
            "medicationReference": {"reference": "Medication/med-1"},
            "authoredOn": "2024-12-05T18:10:16Z",
            "encounter": {"reference": "Encounter/enc-1"},
            "dosageInstruction": [{
                "route": {"coding": [{"system": "http://snomed.info/sct", "code": "26643006", "display": "Oral"}]},
                "doseAndRate": [{"doseQuantity": {"value": 1, "unit": "tablet", "code": "{tbl}"}}],
                "patientInstruction": "1 tablet daily",
            }],
        }
        medications_by_id = {
            "med-1": {"code": {
                "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "10582"}],
                "text": "levothyroxine 75 mcg tablet"}}
        }
        rows = extract_source_medication_fhir([medication_request], medications_by_id, PERSON_ID, SOURCE_SYSTEM)
        self.assertEqual(1, len(rows))
        row = rows[0]
        self.assertEqual("10582", row["s_drug_code"])
        self.assertEqual("RxNorm", row["s_drug_code_type"])
        self.assertEqual("levothyroxine 75 mcg tablet", row["s_drug_text"])
        self.assertEqual(1, row["s_dose"])
        self.assertEqual("tablet", row["s_dose_unit"])
        self.assertEqual("fhir:Encounter/enc-1", row["s_encounter_id"])

    def test_extract_source_medication_without_referenced_medication_falls_back_to_display(self):
        medication_request = {
            "id": "mr-2",
            "medicationReference": {"reference": "Medication/missing", "display": "some drug"},
        }
        rows = extract_source_medication_fhir([medication_request], {}, PERSON_ID, SOURCE_SYSTEM)
        self.assertEqual("some drug", rows[0]["s_drug_text"])


class TestExtractSourceNote(unittest.TestCase):
    def test_skips_short_placeholder_attachments(self):
        import base64
        document_reference = {
            "id": "dref-1",
            "content": [{"attachment": {"contentType": "text/html", "url": "Binary/bin-1"}}],
        }
        binaries_by_id = {"bin-1": {"contentType": "text/html",
                                    "data": base64.standard_b64encode(b"<html></html>").decode()}}
        rows = extract_source_note_fhir([document_reference], binaries_by_id, PERSON_ID, SOURCE_SYSTEM, "/tmp")
        self.assertEqual(0, len(rows))

    def test_extracts_real_note_and_strips_html(self):
        import base64
        import shutil
        import tempfile

        real_html = ("<html><body><p>Chief complaint</p><p>" + ("Patient doing well. " * 20) + "</p></body></html>").encode()
        document_reference = {
            "id": "dref-2",
            "date": "2024-11-26T08:53:58Z",
            "context": {"encounter": [{"reference": "Encounter/enc-1"}]},
            "type": {"coding": [{"system": "http://loinc.org", "code": "11506-3", "display": "Progress note"}]},
            "author": [{"reference": "Practitioner/prac-1"}],
            "content": [{"attachment": {"contentType": "text/html", "url": "Binary/bin-2"}}],
        }
        binaries_by_id = {"bin-2": {"contentType": "text/html",
                                    "data": base64.standard_b64encode(real_html).decode()}}
        tmp_dir = tempfile.mkdtemp()
        try:
            rows = extract_source_note_fhir([document_reference], binaries_by_id, PERSON_ID, SOURCE_SYSTEM, tmp_dir)
            self.assertEqual(1, len(rows))
            row = rows[0]
            self.assertIn("Chief complaint", row["s_note_text"])
            self.assertNotIn("<p>", row["s_note_text"])
            self.assertEqual("fhir:Encounter/enc-1", row["s_encounter_id"])
            self.assertEqual("11506-3", row["s_note_class_code"])
            self.assertEqual("fhir:Practitioner/prac-1", row["k_provider"])
        finally:
            shutil.rmtree(tmp_dir)


class TestExtractSourceCondition(unittest.TestCase):
    def test_no_info_allergy_is_excluded(self):
        allergy = {
            "id": "allergy-1",
            "code": {"coding": [{"system": "http://snomed.info/sct", "code": "1631000175102",
                                 "display": "Patient not asked"}], "text": "Not on File"},
            "clinicalStatus": {"coding": [{"code": "active"}]},
        }
        rows = extract_source_condition_fhir([allergy], PERSON_ID, SOURCE_SYSTEM)
        self.assertEqual(1, len(rows))
        self.assertEqual(1, rows[0]["i_exclude"])

    def test_real_allergy_is_not_excluded(self):
        allergy = {
            "id": "allergy-2",
            "code": {"coding": [{"system": "http://snomed.info/sct", "code": "91936005",
                                 "display": "Allergy to penicillin"}], "text": "Penicillin allergy"},
            "clinicalStatus": {"coding": [{"code": "active"}]},
        }
        rows = extract_source_condition_fhir([allergy], PERSON_ID, SOURCE_SYSTEM)
        self.assertIsNone(rows[0]["i_exclude"])


class TestCodeCrosswalk(unittest.TestCase):
    def test_missing_file_returns_empty_dict(self):
        self.assertEqual({}, load_code_crosswalk("/no/such/file.csv"))

    def test_bundled_crosswalk_loads_and_has_known_entries(self):
        crosswalk = load_code_crosswalk(DEFAULT_CODE_CROSSWALK_PATH)
        self.assertGreater(len(crosswalk), 0)
        tsh = crosswalk[("1.2.840.114350.1.13.717.2.7.5.737384.263", "21705740")]
        self.assertEqual("3016-3", tsh["m_code"])
        self.assertEqual("LOINC", tsh["m_code_type"])
        self.assertEqual("2.16.840.1.113883.6.1", tsh["m_code_type_oid"])

    def test_extract_source_result_applies_crosswalk(self):
        crosswalk = {("epic-oid", "999"): {"m_code": "3016-3", "m_code_type": "LOINC",
                                           "m_code_type_oid": "2.16.840.1.113883.6.1"}}
        observation = {
            "id": "obs-tsh",
            "code": {"coding": [{"system": "urn:oid:epic-oid", "code": "999"}], "text": "(HIS) TSH"},
            "effectiveDateTime": "2022-01-01T00:00:00Z",
            "valueQuantity": {"value": 3.3, "unit": "mIU/L"},
        }
        rows = extract_source_result_fhir([observation], PERSON_ID, SOURCE_SYSTEM, crosswalk)
        self.assertEqual("999", rows[0]["s_code"])
        self.assertEqual("3016-3", rows[0]["m_code"])
        self.assertEqual("LOINC", rows[0]["m_code_type"])

    def test_extract_source_result_without_crosswalk_hit_leaves_m_code_blank(self):
        observation = {
            "id": "obs-other",
            "code": {"coding": [{"system": "urn:oid:epic-oid", "code": "not-in-crosswalk"}]},
        }
        rows = extract_source_result_fhir([observation], PERSON_ID, SOURCE_SYSTEM, {})
        self.assertIsNone(rows[0]["m_code"])

    def test_extract_source_procedure_crosswalk_replaces_source_code(self):
        crosswalk = {("epic-proc-oid", "135591"): {"m_code": "40701008", "m_code_type": "SNOMED",
                                                    "m_code_type_oid": "2.16.840.1.113883.6.96"}}
        procedure = {
            "id": "proc-echo",
            "code": {"coding": [{"system": "urn:oid:epic-proc-oid", "code": "135591",
                                 "display": "ECHOCARDIOGRAM 2D M-MODE DOPPLER"}]},
            "performedDateTime": "2020-01-01T00:00:00Z",
        }
        rows = extract_source_procedure_fhir([procedure], PERSON_ID, SOURCE_SYSTEM, crosswalk)
        self.assertEqual("40701008", rows[0]["s_procedure_code"])
        self.assertEqual("SNOMED", rows[0]["s_procedure_code_type"])
        self.assertEqual("2.16.840.1.113883.6.96", rows[0]["s_procedure_code_type_oid"])


if __name__ == "__main__":
    unittest.main()
