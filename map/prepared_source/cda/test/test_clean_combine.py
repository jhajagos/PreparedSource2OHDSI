import csv
import json
import os
import shutil
import tempfile
import unittest

import preparedsource2ohdsi.prepared_source as ps

from ..clean_combine_prepared_source_fragments import main


def _write_fragment_csv(path, rows):
    header = list(rows[0].keys())
    with open(path, mode="w", newline="") as fw:
        writer = csv.DictWriter(fw, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _make_source_directory(root, person_id, fragments):
    """fragments: {table_name: [row_dict, ...]}. Writes each table's rows to one fragment CSV and
    a manifest referencing it, mirroring what the CDA/FHIR extractors produce."""
    output_root = os.path.join(root, "output")
    frag_dir = os.path.join(output_root, "ps_frags")
    os.makedirs(frag_dir, exist_ok=True)

    manifest = {"s_person_id": person_id, "prepared_source": {}, "fragments": {}}
    for table_name, rows in fragments.items():
        frag_path = os.path.join(frag_dir, f"{table_name}.csv")
        _write_fragment_csv(frag_path, rows)
        manifest["fragments"][table_name] = [frag_path]

    with open(os.path.join(output_root, "s_files_generated.json"), "w") as fw:
        json.dump(manifest, fw)


class TestMultiDirectoryCombine(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.person_id = "shared-person-id"

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_merges_fragments_from_two_directories_for_the_same_person(self):
        cda_dir = os.path.join(self.tmp_dir, "cda")
        fhir_dir = os.path.join(self.tmp_dir, "fhir")
        merged_dir = os.path.join(self.tmp_dir, "merged")

        person_row = ps.SourcePersonObject().dict_template()
        person_row["s_person_id"] = self.person_id
        person_row["s_gender"] = "Female"
        _make_source_directory(cda_dir, self.person_id, {"source_person": [person_row]})

        encounter_row = ps.SourceEncounterObject().dict_template()
        encounter_row["s_encounter_id"] = "fhir:Encounter/enc-1"
        encounter_row["s_person_id"] = self.person_id
        encounter_row["s_visit_start_datetime"] = "2022-01-01"
        _make_source_directory(fhir_dir, self.person_id, {"source_encounter": [encounter_row]})

        main([cda_dir, fhir_dir], merged_dir)

        merged_ps_dir = os.path.join(merged_dir, "output", "ps")
        self.assertTrue(os.path.exists(os.path.join(merged_ps_dir, "source_person.csv")))
        self.assertTrue(os.path.exists(os.path.join(merged_ps_dir, "source_encounter.csv")))

        with open(os.path.join(merged_ps_dir, "source_person.csv")) as f:
            rows = list(csv.DictReader(f))
        self.assertEqual(1, len(rows))
        self.assertEqual("Female", rows[0]["s_gender"])

        with open(os.path.join(merged_ps_dir, "source_encounter.csv")) as f:
            rows = list(csv.DictReader(f))
        self.assertEqual(1, len(rows))
        self.assertEqual("fhir:Encounter/enc-1", rows[0]["s_encounter_id"])

        with open(os.path.join(merged_ps_dir, "source_observation_period.csv")) as f:
            rows = list(csv.DictReader(f))
        self.assertEqual(self.person_id, rows[0]["s_person_id"])

    def test_first_directory_wins_for_source_person_conflicts(self):
        cda_dir = os.path.join(self.tmp_dir, "cda")
        fhir_dir = os.path.join(self.tmp_dir, "fhir")
        merged_dir = os.path.join(self.tmp_dir, "merged")

        cda_person = ps.SourcePersonObject().dict_template()
        cda_person["s_person_id"] = self.person_id
        cda_person["s_gender"] = "Male"
        _make_source_directory(cda_dir, self.person_id, {"source_person": [cda_person]})

        fhir_person = ps.SourcePersonObject().dict_template()
        fhir_person["s_person_id"] = self.person_id
        fhir_person["s_gender"] = "female"
        _make_source_directory(fhir_dir, self.person_id, {"source_person": [fhir_person]})

        main([cda_dir, fhir_dir], merged_dir)

        with open(os.path.join(merged_dir, "output", "ps", "source_person.csv")) as f:
            rows = list(csv.DictReader(f))
        self.assertEqual("Male", rows[0]["s_gender"])

    def test_mismatched_person_ids_raise(self):
        cda_dir = os.path.join(self.tmp_dir, "cda")
        fhir_dir = os.path.join(self.tmp_dir, "fhir")

        cda_person = ps.SourcePersonObject().dict_template()
        cda_person["s_person_id"] = "person-a"
        _make_source_directory(cda_dir, "person-a", {"source_person": [cda_person]})

        fhir_person = ps.SourcePersonObject().dict_template()
        fhir_person["s_person_id"] = "person-b"
        _make_source_directory(fhir_dir, "person-b", {"source_person": [fhir_person]})

        with self.assertRaises(ValueError):
            main([cda_dir, fhir_dir], os.path.join(self.tmp_dir, "merged"))

    def test_single_directory_still_defaults_output_in_place(self):
        cda_dir = os.path.join(self.tmp_dir, "cda")
        person_row = ps.SourcePersonObject().dict_template()
        person_row["s_person_id"] = self.person_id
        _make_source_directory(cda_dir, self.person_id, {"source_person": [person_row]})

        main([cda_dir])

        self.assertTrue(os.path.exists(os.path.join(cda_dir, "output", "ps", "source_person.csv")))


if __name__ == "__main__":
    unittest.main()
