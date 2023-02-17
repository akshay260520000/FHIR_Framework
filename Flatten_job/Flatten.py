import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
import json
from pyspark.sql.functions import explode, explode_outer, col, when, split,  \
    concat_ws, collect_set, udf, element_at, to_date, lit, desc, dense_rank

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("PySpark Read JSON") \
    .getOrCreate()

with open('/workspaces/FHIR_Framework/Config_files/hcs_config.json') as config_file:
    config=json.load(config_file)["Source_Schema"]


schema = StructType.fromJson(config)

input_path ='/workspaces/FHIR_Framework/Input_data_files/hcs_json.json'

hcs_df = spark.read.schema(schema).format("json").option("multiLine", "true").load(input_path)
hcs_df.printSchema()
hcs_df.show(truncate=False)

print('---------------------Target Schema-----------------')
with open('/workspaces/FHIR_Framework/Config_files/hcs_config.json') as config_file:
    targetschema=json.load(config_file)["targetschema"]
targetschema_df = hcs_df.select(*(targetschema))
targetschema_df.show(truncate=False)

print("-------------------------Explode Level 1------------------")
with open('/workspaces/FHIR_Framework/Config_files/hcs_config.json') as config_file:
    explode1=json.load(config_file)["explode_1"]


explode1_df = targetschema_df.withColumn(f"new_{explode1}",explode_outer(explode1)).drop(col(f"{explode1}"))
explode1_df.show(truncate=False)