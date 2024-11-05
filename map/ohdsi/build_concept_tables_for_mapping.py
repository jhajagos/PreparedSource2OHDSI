from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import json
import logging
import argparse

import preparedsource2ohdsi.mapping_utilities as mapping_utilities

logging.basicConfig(level=logging.INFO)


def main(config, evaluate_samples=False):
    """Build Optimized Concept Tables for Mapping to OHDSI and loading standard tables to Parquet"""

    concept_base_path = config["concept_base_path"]
    csv_file_extension = config["concept_csv_file_extension"]

    concept_sdf_spark_path = f"{concept_base_path}CONCEPT{csv_file_extension}"

    logging.info(f"Reading concept csv: '{concept_sdf_spark_path}'")

    concept_sdf = spark.read.option("inferSchema", "true").\
        option("header", "true").option("delimiter", "\t").\
        csv(concept_sdf_spark_path)

    concept_sdf = concept_sdf.repartition("vocabulary_id", "domain_id")
    concept_sdf.createOrReplaceTempView("concept")

    concept_relationship_spark_path = f"{concept_base_path}CONCEPT_RELATIONSHIP{csv_file_extension}"
    logging.info(f"Reading concept_relationship csv: '{concept_relationship_spark_path}'")

    concept_relationship_sdf = spark.read.option("inferSchema", "true").\
        option("header", "true").option("delimiter", "\t").\
        csv(concept_relationship_spark_path)

    vocabulary_spark_path = f"{concept_base_path}VOCABULARY{csv_file_extension}"
    logging.info(f"Reading vocabulary csv: f'{vocabulary_spark_path}'")

    vocabulary_sdf = spark.read.option("inferSchema", "true"). \
        option("header", "true").option("delimiter", "\t"). \
        csv(vocabulary_spark_path)

    concept_relationship_sdf.repartition("relationship_id")
    concept_relationship_sdf.createOrReplaceTempView("concept_relationship")

    # This builds a table with all mappings from source to mapped concepts
    concept_map_sdf = concept_sdf.alias("c1").\
        join(concept_relationship_sdf.alias("cr").
             filter(F.col("cr.relationship_id").isin(["Maps to value", "Maps to"])),
             F.col("c1.concept_id") == F.col("cr.concept_id_1")).\
        join(concept_sdf.alias("c2"), F.col("cr.concept_id_2") == F.col("c2.concept_id")).\
        join(vocabulary_sdf.alias("v1"), F.col("v1.vocabulary_id") == F.col("c1.vocabulary_id")).\
        join(vocabulary_sdf.alias("v2"), F.col("v2.vocabulary_id") == F.col("c2.vocabulary_id")).\
        select(
        F.col("c1.concept_id").alias("source_concept_id"),
        F.col("c1.vocabulary_id").alias("source_vocabulary_id"),
        F.col("v1.vocabulary_version").alias("source_vocabulary_version"),
        F.col("c1.domain_id").alias("source_domain_id"),
        F.col("c1.concept_code").alias("source_concept_code"),
        F.col("c1.standard_concept").alias("source_standard_concept"),
        F.col("c1.concept_name").alias("source_concept_name"),
        F.col("cr.relationship_id"),
        F.col("c2.concept_id").alias("mapped_concept_id"),
        F.col("c2.vocabulary_id").alias("mapped_vocabulary_id"),
        F.col("v2.vocabulary_version").alias("mapped_vocabulary_version"),
        F.col("c2.domain_id").alias("mapped_domain_id"),
        F.col("c2.standard_concept").alias("mapped_standard_concept"),
        F.col("c2.concept_code").alias("mapped_concept_code"),
        F.col("c2.concept_name").alias("mapped_concept_name")
        )

    if evaluate_samples:
        concept_map_sdf.filter(F.col("source_vocabulary_id") == F.lit("ICD10CM")).sample(0.1).limit(1000).toPandas().to_csv("./sample_vocabulary_map_ohdsi.csv", index=False)

    concept_map_sdf = concept_map_sdf.repartition("source_vocabulary_id")
    tables_to_export = {"concept": concept_sdf, "concept_map": concept_map_sdf}

    tables_to_export["vocabulary"] = vocabulary_sdf

    tables_to_export["concept_relationship"] = concept_relationship_sdf

    # Load additional tables
    concept_ancestor_sdf_spark_path = f"{concept_base_path}CONCEPT_ANCESTOR{csv_file_extension}"
    concept_ancestor_sdf = spark.read.option("inferSchema", "true").\
        option("header", "true").option("delimiter", "\t").\
        csv(concept_ancestor_sdf_spark_path)
    tables_to_export["concept_ancestor"] = concept_ancestor_sdf

    concept_class_sdf_spark_path = f"{concept_base_path}CONCEPT_CLASS{csv_file_extension}"
    concept_class_sdf = spark.read.option("inferSchema", "true"). \
        option("header", "true").option("delimiter", "\t"). \
        csv(concept_class_sdf_spark_path)
    tables_to_export["concept_class"] = concept_class_sdf

    concept_synonym_sdf_spark_path = f"{concept_base_path}CONCEPT_SYNONYM{csv_file_extension}"
    concept_synonym_sdf = spark.read.option("inferSchema", "true").\
        option("header", "true").option("delimiter", "\t").\
        csv(concept_synonym_sdf_spark_path)

    tables_to_export["concept_synonym"] = concept_synonym_sdf

    drug_strength_sdf_spark_path = f"{concept_base_path}DRUG_STRENGTH{csv_file_extension}"
    drug_strength_sdf = spark.read.option("inferSchema", "true"). \
        option("header", "true").option("delimiter", "\t"). \
        csv(drug_strength_sdf_spark_path)
    tables_to_export["drug_strength"] = drug_strength_sdf

    domain_sdf_spark_path = f"{concept_base_path}DOMAIN{csv_file_extension}"
    domain_sdf = spark.read.option("inferSchema", "true"). \
        option("header", "true").option("delimiter", "\t"). \
        csv(domain_sdf_spark_path)
    tables_to_export["domain"] = domain_sdf

    relationship_sdf_path = f"{concept_base_path}RELATIONSHIP{csv_file_extension}"
    relationship_sdf = spark.read.option("inferSchema", "true"). \
        option("header", "true").option("delimiter", "\t"). \
        csv(relationship_sdf_path)
    tables_to_export["relationship"] = relationship_sdf

    mapping_utilities.export_sdf_dict_to_parquet(tables_to_export, config["export_concept_mapping_table_path"],
                                                 {"concept": "vocabulary_id", "concept_map": "source_vocabulary_id",
                                                  "concept_relationship": "relationship_id",
                                                  "concept_ancestor": "min_levels_of_separation"
                                                  })


if __name__ == '__main__':
    arg_parser_obj = argparse.ArgumentParser(
        description="Stage (convert to Parquet) and preprocess for mapping OHDSI CDM files")
    arg_parser_obj.add_argument("-c", "--config-json-file-name", dest="config_json_file_name", default="./config.json")
    arg_parser_obj.add_argument("-l", "--run-local", dest="run_local", default=False, action="store_true")

    arg_obj = arg_parser_obj.parse_args()
    RUN_LOCAL = arg_obj.run_local

    with open(arg_obj.config_json_file_name) as f:
        config_dict = json.load(f)

    logging.info("Configuration:")
    logging.info(config_dict)

    # If concept_base_path is not provided uses the next higher path
    if "concept_base_path" not in config_dict:
        if "export_concept_mapping_table_path" in config_dict:
            export_path = config_dict["export_concept_mapping_table_path"]
            concept_base_path = "/".join(export_path.split("/")[:-2])
            concept_base_path += "/"
            config_dict["concept_base_path"] = concept_base_path

    if RUN_LOCAL:
        spark = SparkSession.builder \
            .config('spark.driver.extraJavaOptions', '-Duser.timezone=GMT') \
            .config('spark.executor.extraJavaOptions', '-Duser.timezone=GMT') \
            .config('spark.sql.session.timeZone', 'UTC') \
            .config("spark.driver.memory", "16g") \
            .getOrCreate()
    else:
        spark = SparkSession.builder \
            .config('spark.driver.extraJavaOptions', '-Duser.timezone=GMT') \
            .config('spark.executor.extraJavaOptions', '-Duser.timezone=GMT') \
            .config('spark.sql.session.timeZone', 'UTC') \
            .getOrCreate()

    main(config_dict)

