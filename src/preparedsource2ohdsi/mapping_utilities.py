import pyspark.sql.functions as F
import logging
import csv
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType


# Map fields using a dictionary
def map_table_column_names(table_obj, column_map, include_unmatched_columns=False):
    """Helper function for using a dictionary for renaming column names from a table"""
    mapped_columns_list = []

    if not(include_unmatched_columns):
        for column_name in column_map:
            mapped_columns_list += [F.col(column_name).alias(column_map[column_name])]

    else:
        original_column_names = table_obj.columns
        for column_name in original_column_names:
            if column_name in column_map:
                mapped_columns_list += [F.col(column_name).alias(column_map[column_name])]
            else:
                mapped_columns_list += [F.col(column_name)]

    return table_obj.select(*mapped_columns_list)


def column_names_to_align_to(table_obj, output_table_obj, include_unmatched_columns=False):
    """Align column names to a templated source"""

    current_columns = table_obj.columns
    column_list = output_table_obj.fields()

    try:
        data_types = output_table_obj.data_types()
    except AttributeError:
        data_types = {}

    select_list = []
    columns_not_selected = []

    for column in column_list:
        if column in data_types:
            data_type = data_types[column]
        else:
            data_type = "string"

        select_list += [F.lit(None).cast(data_type).alias(column)]

    for column in current_columns:
        if column in column_list:
            select_list[column_list.index(column)] = F.col(column)
        else:
            columns_not_selected += [column]

    if include_unmatched_columns:
        for column in columns_not_selected:
            select_list += [F.col(column)]

    return table_obj.select(*select_list)


def load_true_csv_file(spark_ptr, file_path):
    """Load a true CSV file delimited by a ','"""

    logging.info(f"Loading: '{file_path}'")
    return spark_ptr.read.option("inferSchema", "true").\
        option("header", "true"). \
        option("multiline", "true"). \
        option("quote", '"'). \
        option("escape", "\\") .\
        option("escape", '"') .\
        csv(file_path)


def load_csv_to_sparc_df_dict(spark_ptr, file_dict, file_type, base_path, extension, lowercase_columns=True):
    """Helper function for loading a dictionary of tables into Spark dataframe; spark_ptr is a pointer
    to the spark instance"""
    sdf_dict = {}

    for file_key in file_dict:
        if file_type in ("csv",):
            sdf_dict[file_key] = load_true_csv_file(spark_ptr, base_path + file_dict[file_key] + extension)
            if lowercase_columns:
                columns = sdf_dict[file_key].columns
                select_list = []
                for c in columns:
                    select_list += [F.col(c).alias(c.lower())]

                sdf_dict[file_key] = sdf_dict[file_key].select(*select_list)
        else:
            raise NotImplementedError

    return sdf_dict


def load_local_csv_file(spark_ptr, path_to_file, table_name=None):
    """A quick way to load a table"""
    with open(path_to_file, newline="", mode="r") as f:
        csv_reader = csv.reader(f)
        header = next(csv_reader)
        sdf = spark_ptr.createDataFrame(data=list(csv_reader), schema=header)
        if table_name is not None:
            sdf.createOrReplaceTempView(table_name)
        return sdf


def export_sdf_dict_to_parquet(sdf_dict, base_export_base_path, partitions_by={}):
    """Loads a dictionary where the key is the table_name and points to a spark dataframe; second parameter is the
    location to write file to"""

    for table_name in sdf_dict:
        sdf = sdf_dict[table_name]
        spark_path_to_file = base_export_base_path + table_name + ".parquet"
        logging.info(f"Exporting: '{table_name}' to '{spark_path_to_file}'")
        if table_name in partitions_by:
            partition_by = partitions_by[table_name]
            sdf.repartition(partition_by).write.partitionBy(partition_by).mode('overwrite').parquet(spark_path_to_file)
        else:
            sdf.write.mode('overwrite').parquet(spark_path_to_file)


def load_parquet_from_table_names(spark_ptr, table_names, base_path, extension=".parquet", cache=False):
    """Loads parquet tables that have been exported with export_sdf_dict_to_parquet into a dict"""

    sdf_dict = {}
    for table_name in table_names:
        spark_path_to_file = f"{base_path}{table_name}{extension}"
        logging.info(f"Loading '{table_name}' from '{spark_path_to_file}'")
        sdf_dict[table_name] = spark_ptr.read.parquet(spark_path_to_file)
        if cache:
            sdf_dict[table_name] = sdf_dict[table_name].cache()

    return sdf_dict


def dict_to_dataframe(spark_ptr, dictionary, column_pair):
    """Converts a dictionary into a two column spark DataFrame"""

    collecting_list = []
    for key in dictionary:
        collecting_list += [[key, dictionary[key]]]

    return spark_ptr.createDataFrame(collecting_list, schema=column_pair)


def json_dict_to_dataframe(spark_ptr, json_file_name, column_pair):
    """Converts a dictionary in a JSON file to two column Spark DataFrame"""
    with open(json_file_name) as f:
        dictionary = json.load(f)

    return dict_to_dataframe(spark_ptr, dictionary, column_pair)


def write_parquet_file_and_reload(spark_ptr, sdf, table_name, output_location):
    """Write Spark Dataframe to a Parquet and then reload it"""

    parquet_path = output_location + table_name + ".parquet"
    sdf.write.mode("overwrite").parquet(parquet_path)

    logging.info(f"Writing '{parquet_path}'")

    sdf = spark_ptr.read.parquet(parquet_path)

    logging.info(f"Reading '{parquet_path}'")

    return sdf, parquet_path


def distinct_and_add_row_id(sdf):

    sdf = sdf.distinct()
    sdf = sdf.withColumn("g_id", F.monotonically_increasing_id())

    return sdf


def create_schema_from_table_object(table_object, add_g_id):
    field_list = table_object.fields()
    try:
        data_types = table_object.data_types()
    except AttributeError:
        data_types = {}

    data_type_map = {"VARCHAR": StringType(), "INTEGER": IntegerType(), "DATE": DateType(),
                     "TIMESTAMP": TimestampType()}

    schema_table_columns = []
    for field in field_list:
        if field in data_types:
            field_data_type_name = data_types[field]
            if "VARCHAR" in field_data_type_name:
                field_data_type = StringType()
            else:
                if field_data_type_name in data_type_map:
                    field_data_type = data_type_map[field_data_type_name]
                else:
                    field_data_type = StringType()
        else:
            field_data_type = StringType()

        schema_table_columns += [StructField(field, field_data_type, True)]

    if add_g_id:
        schema_table_columns += [StructField("g_id", IntegerType(), True)]

    schema = StructType(schema_table_columns)

    return schema


def create_empty_table_from_table_object(spark, table_object, add_g_id=True):

    schema = create_schema_from_table_object(table_object, add_g_id)

    empty_rdd = spark.sparkContext.emptyRDD()

    return spark.createDataFrame(empty_rdd, schema)


def attach_catalog_dict(spark_ptr, table_catalog, domains_to_exclude=None):
  """Takes an output dictionary from the OHDSI mapper and maps tables in the domain"""

  db_cat = {}
  if domains_to_exclude is not None:
      filtered_domains = [dn for dn in table_catalog if dn not in domains_to_exclude]
  else:
      filtered_domains = list(table_catalog.keys())

  for domain in filtered_domains:
    for table in table_catalog[domain]:
      location = table_catalog[domain][table]
      if domain not in db_cat:
          db_cat[domain] = {}
      logging.info(f"Loading '{location}' and attaching to namespace as '{table}'")
      db_cat[domain][table] = spark_ptr.read.parquet(location)
      db_cat[domain][table].createOrReplaceTempView(table)

  return db_cat