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
df = spark.read.schema(schema).format("json").option("multiLine", "true").load(input_path)

def Addcolumns(df,colums):
    for colm in colums:
        newvalue=colm.replace('.','_')
        df=df.withColumn(f'{newvalue}',col(colm))
    return df

print('---------------------Target Schema-----------------')
struct_explode_dict = con["struct_explode_dict"]
array_explode_dict = con["array_explode_dict"]
for i in range(1,(len(struct_explode_dict)+1)):
    
    df=Addcolumns(df,struct_explode_dict[f'{i}'])            
    if(i<=len(array_explode_dict)):
        for column in array_explode_dict[f'{i}']:
            df = df.withColumn(f"new_{column}",explode_outer(column)).\
                drop(col(f"{column}")).withColumnRenamed(f'new_{column}',f'{column}')
final_target_schema=con["final_target_schema"]
df=df.select(*final_target_schema)
df.show()