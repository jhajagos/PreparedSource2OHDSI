import pathlib
import os
import unittest
from ..cda_to_prepared_source import generate_patient_identifier, parse_xml_file, extract_source_medication_ccda


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
        self.d1 = parse_xml_file(current_directory / "samples/patient_1/Transition_of_Care_Referral_Summary.xml")

    def test_extract_source_meds(self):
        result = extract_source_medication_ccda(self.d1)

        print(result)

        self.assertNotEqual(0, len(result))



if __name__ == '__main__':
    unittest.main()
