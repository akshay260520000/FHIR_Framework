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
    con=json.load(config_file)

schema_temp = con["Source_Schema"]
schema = StructType.fromJson(schema_temp)





input_path ='/workspaces/FHIR_Framework/Input_data_files/hcs_json.json'

hcs_df = spark.read.schema(schema).format("json").option("multiLine", "true").load(input_path)
hcs_df.printSchema()
hcs_df.show(truncate=False)

print('---------------------Target Schema-----------------')
targetschema = con["targetschema"]
targetschema_df = hcs_df.select(*(targetschema))
targetschema_df.show(truncate=False)

print("-------------------------Explode Level 1------------------")
explode1 = con["explode_1"]
explode1_df = targetschema_df.withColumn(f"new_{explode1}",explode_outer(explode1)).drop(col(f"{explode1}"))
explode1_df.show(truncate=False)