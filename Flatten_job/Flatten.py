import sys
import pyspark
from pyspark import SparkContext
from pyspark.sql.types import *
import json
from pyspark.sql.functions import explode, explode_outer, col, when, split,  \
    concat_ws, collect_set, udf, element_at, to_date, lit, desc, dense_rank
sc = SparkContext()



with open('/workspaces/codespaces-blank/Config_files/hcs_config.json') as config_file:
    config = json.loads(config_file)


schema = config["schema"]
schema = StructType.fromJson(schema)
targetschema = config["targetschema"]


input_path ='/workspaces/codespaces-blank/Input_data_files/hcs_json.json'


hcs_df = sc.read.schema(schema).format("json").option("multiLine", "true").load(input_path)
hcs_df.printSchema()
hcs_df.show(truncate=False)

print('---------------------Target Schema-----------------')
targetschema_df = hcs_df.select(*(targetschema))
targetschema_df.show(truncate=False)

print("-------------------------Explode Level 1------------------")
explode1 = config["explode_1"]
explode1_df = targetschema_df.withColumn("{str(explode1)}",explode_outer(explode1)).drop(col("{str(explode1)}"))
explode1_df.show(truncate=False)

