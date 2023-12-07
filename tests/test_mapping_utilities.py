import unittest
from preparedsource2ohdsi.mapping_utilities import (map_table_column_names, column_names_to_align_to,
                                                    load_local_csv_file, dict_to_dataframe)
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


class OutputObject(object):
    def __init__(self):
        pass

    def fields(self):
        return ["name", "uid", "reference"]


class TestTableOperations(unittest.TestCase):

    def test_mapping_column_names(self):

        columns = ["uid", "name", "description"]
        data = [
                ["d67lddjl5", "Rabbit", "A placental mammals that hops on four legs"],
                ["ddddgggg5", "Kangaroo", "A marsupial that hops on two legs"]
               ]

        sdf1 = spark.createDataFrame(data=data, schema=columns)
        sdf1 = map_table_column_names(sdf1, {"uid": "unique_identifier", "name": "full_name"})
        sdf1_columns = sdf1.columns

        self.assertEqual(["unique_identifier", "full_name"], sdf1_columns)

        sdf2 = spark.createDataFrame(data=data, schema=columns)
        sdf2 = map_table_column_names(sdf2, {"uid": "unique_identifier", "name": "full_name"}, include_unmatched_columns=True)
        sdf2_columns = sdf2.columns

        self.assertEqual(["unique_identifier", "full_name", "description"], sdf2_columns)

    def test_column_names_to_align_to(self):
        columns = ["uid", "name", "description"]
        data = [
            ["d67lddjl5", "Rabbit", "A placental mammals that hops on four legs"],
            ["ddddgggg5", "Kangaroo", "A marsupial that hops on two legs"]
        ]

        sdf1 = spark.createDataFrame(data=data, schema=columns)

        sdf2 = column_names_to_align_to(sdf1, OutputObject())

        sdf2_columns = sdf2.columns

        self.assertEqual(["name", "uid", "reference"], sdf2_columns)

        sdf3 = column_names_to_align_to(sdf1, OutputObject(), include_unmatched_columns=True)
        sdf3_columns = sdf3.columns

        self.assertEqual(["name", "uid", "reference", "description"], sdf3_columns)

    def test_load_local_csv_file(self):

        csv_sdf = load_local_csv_file(spark, "./gender_mappings.csv")

        self.assertEqual(["concept_name", "concept_code"], csv_sdf.columns)

        self.assertEqual(2, csv_sdf.count())

    def test_dict_to_dataframe(self):

        dict_to_convert = {"A": 5, "B": 11, "Z": 22}

        sdf = dict_to_dataframe(spark, dict_to_convert, ("letter", "number"))

        self.assertEqual(3, sdf.count())

        self.assertEqual(["letter", "number"], sdf.columns)

        result = sdf.toPandas().values.tolist()

        self.assertEqual(["A", 5], result[0])


if __name__ == '__main__':
    unittest.main()
