import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
import json
from pyspark.sql.functions import explode, explode_outer, col, when, split,  \
    concat_ws, collect_set, udf, element_at, to_date, lit, desc, dense_rank,\
    monotonically_increasing_id

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("PySpark Read JSON") \
    .getOrCreate()

with open('/workspaces/FHIR_Framework/Config_files/practioner_config.json') as config_file:
    con=json.load(config_file)

schema_temp = con["Source_Schema"]
schema = StructType.fromJson(schema_temp)

input_path ='/workspaces/FHIR_Framework/Input_data_files/practioner_json.json'

hcs_df = spark.read.schema(schema).format("json").option("multiLine", "true").load(input_path)
hcs_df.printSchema()


print('---------------------Target Schema-----------------')
targetschema_dict = con["targetschemadict"]
explode_dict = con["explodedict"]

for i in range(1,(len(targetschema_dict)+1)):
    
    hcs_df =hcs_df.select("*",*(targetschema_dict[f'{i}']))
    
    
    
    if(i<=len(explode_dict)):
        for column in explode_dict[f'{i}']:
            hcs_df = hcs_df.withColumn(f"new_{column}",explode_outer(column)).\
                drop(col(f"{column}")).withColumnRenamed(f'new_{column}',f'{column}')
print("______Final_________")
hcs_df.show(5)

      
     


