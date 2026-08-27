import pathlib
import os
import unittest
import xml.etree.ElementTree as et
from ..cda_to_prepared_source_fragments import (generate_patient_identifier, parse_xml_file,
                                                extract_source_medication_ccda, extract_labs_source_result_ccda,
                                                extract_source_person_ccda, extract_vitals_source_result_ccda,
                                                extract_problems_source_condition_ccda,
                                                extract_source_procedures_ccda,
                                                extract_source_encounter_ccda, extract_source_care_site_ccda,
                                                extract_source_location_ccda, extract_source_provider_ccda,
                                                classify_visit_type_ccda,
                                                )


file_directory = __file__
current_directory = pathlib.Path(os.path.abspath(os.path.split(file_directory)[0]))


class TestBasicFunctionality(unittest.TestCase):

    def test_parse_xml_file(self):
        cda = parse_xml_file(current_directory / "samples/patient_1/Transition_of_Care_Referral_Summary.xml")
        self.assertIsNotNone(cda)

    def test_pid(self):

        pid = generate_patient_identifier("./samples/patient_1", salt="spectacular")
        self.assertIsNotNone(pid)  # add assertion here

        self.assertNotEqual("./samples/patient_1", pid)


class TestPSConversion(unittest.TestCase):

    def setUp(self):
        self.file_name = "samples/patient_1/Transition_of_Care_Referral_Summary.xml"
        self.d1 = parse_xml_file(current_directory / self.file_name)
        self.spid = generate_patient_identifier("samples/patient_1", salt="spectacular")

    def test_extract_source_meds(self):
        results = extract_source_medication_ccda(self.d1, self.spid, self.file_name)

        self.assertNotEqual(0, len(results))

        result_0 = results[0]

        self.assertIsNotNone(result_0["s_person_id"])

        self.assertIsNotNone(result_0["s_drug_code"])

    def test_extract_source_lab_result(self):
        results = extract_labs_source_result_ccda(self.d1, self.spid, self.file_name)

        self.assertNotEqual(0, len(results))

        result_0 = results[0]

        self.assertIsNotNone(result_0["s_person_id"])

        self.assertIsNotNone(result_0["s_code"])

    def test_extract_source_person(self):
        results = extract_source_person_ccda(self.d1, self.spid, self.file_name)
        self.assertEqual(1, len(results))
        result_0 = results[0]

        self.assertIsNotNone(result_0["s_gender_code"])

    def test_extract_source_vitals(self):

        results = extract_vitals_source_result_ccda(self.d1, self.spid, self.file_name)
        self.assertTrue(len(results) > 0)

        self.assertIsNotNone("s_result_numeric")

    def test_extract_source_problems(self):
        results = extract_problems_source_condition_ccda(self.d1, self.spid, self.file_name)
        self.assertTrue(len(results) > 0)

        self.assertIsNotNone("s_result_numeric")

    def test_extract_source_procedures(self):
        results = extract_source_procedures_ccda(self.d1, self.spid, self.file_name)
        print(results)
        self.assertTrue(len(results) > 0)

    def test_extract_source_encounter(self):
        results = extract_source_encounter_ccda(self.d1, self.spid, self.file_name)
        self.assertEqual(1, len(results))
        result_0 = results[0]

        self.assertEqual("c-cda:encounter:2.16.840.1.113883.1.13.99999.2:162", result_0["s_encounter_id"])
        self.assertIsNotNone(result_0["s_visit_start_datetime"])
        self.assertEqual("Inpatient", result_0["s_visit_type"])
        # code is nullFlavor="UNK" (no real ActEncounterCode) -- classified by the "inpatient" keyword
        self.assertEqual("IP", result_0["m_visit_type_code"])
        self.assertEqual("ohdsi.visit", result_0["m_visit_type_code_type_oid"])
        self.assertEqual("c-cda:care_site:Local Community Hospital Organization", result_0["k_care_site"])
        self.assertEqual("c-cda:provider:name:Dale_Owens", result_0["k_provider"])

    def test_extract_source_care_site(self):
        results = extract_source_care_site_ccda(self.d1, self.file_name)
        self.assertEqual(1, len(results))
        self.assertEqual("Local Community Hospital Organization", results[0]["s_care_site_name"])
        self.assertIsNotNone(results[0]["k_location"])

    def test_extract_source_location(self):
        results = extract_source_location_ccda(self.d1, self.file_name)
        self.assertEqual(1, len(results))
        self.assertEqual("Portland", results[0]["s_city"])
        self.assertEqual("OR", results[0]["s_state"])

    def test_extract_source_provider(self):
        results = extract_source_provider_ccda(self.d1, self.file_name)
        self.assertEqual(1, len(results))
        self.assertEqual("c-cda:provider:name:Dale_Owens", results[0]["k_provider"])
        self.assertEqual("Dale Owens", results[0]["s_provider_name"])
        self.assertIsNone(results[0]["s_npi"])


class TestClassifyVisitType(unittest.TestCase):

    def test_direct_act_encounter_code_mapping(self):
        self.assertEqual(("Outpatient Visit", "OP"), classify_visit_type_ccda("AMB", None))
        self.assertEqual(("Emergency Room Visit", "ER"), classify_visit_type_ccda("EMER", None))
        self.assertEqual(("Inpatient Visit", "IP"), classify_visit_type_ccda("IMP", None))

    def test_keyword_fallback_when_no_code(self):
        self.assertEqual(("Inpatient Visit", "IP"), classify_visit_type_ccda(None, "Inpatient"))
        self.assertEqual(("Outpatient Visit", "OP"), classify_visit_type_ccda(None, "Private OP"))
        self.assertEqual(("Outpatient Visit", "OP"), classify_visit_type_ccda(None, None))


class TestEncounterNullFlavorSkip(unittest.TestCase):
    """A couple of files in the real corpus have <encounter><id nullFlavor="NA"/></encounter>
    placeholder stubs buried in a lab-result narrative act -- these carry no extractable data and
    must not become source_encounter rows."""

    ENCOUNTERS_SECTION_XML = """
    <ClinicalDocument xmlns="urn:hl7-org:v3">
      <component>
        <structuredBody>
          <component>
            <section>
              <code code="46240-8" codeSystem="2.16.840.1.113883.6.1" />
              <entry>
                <encounter classCode="ENC" moodCode="EVN">
                  <id nullFlavor="NA" />
                </encounter>
              </entry>
            </section>
          </component>
        </structuredBody>
      </component>
    </ClinicalDocument>
    """

    def setUp(self):
        self.doc = et.ElementTree(et.fromstring(self.ENCOUNTERS_SECTION_XML))

    def test_placeholder_encounter_is_skipped(self):
        self.assertEqual([], extract_source_encounter_ccda(self.doc, "spid", "test.xml"))

    def test_placeholder_encounter_yields_no_care_site_or_provider(self):
        self.assertEqual([], extract_source_care_site_ccda(self.doc, "test.xml"))
        self.assertEqual([], extract_source_provider_ccda(self.doc, "test.xml"))


if __name__ == '__main__':
    unittest.main()
