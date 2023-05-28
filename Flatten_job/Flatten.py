import sys
import json
import pyspark
import csv
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql.functions import explode, explode_outer, col, when, split,  \
    concat_ws, collect_set, udf, element_at, to_date, lit, desc, dense_rank

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("PySpark Read JSON") \
    .config("spark.sql.debug.maxToStringFields", 1000) \
    .getOrCreate()

# Input_File=input("Enter The name of Input file or Input Folder: ")
# Config_File=input("Enter the name of Config_File or Config Folder: ")

try:
    with open('/workspaces/FHIR_Framework/Config_files/org_config.json', encoding='utf-8') \
        as config_file:
        con = json.load(config_file)
except FileNotFoundError:
    print("Error: Config file not found")
    sys.exit(1)
schema_temp = con["Source_Schema"]
schema = StructType.fromJson(schema_temp)
INPUT_PATH = '/workspaces/FHIR_Framework/Input_data_files/org.json'
df = spark.read.schema(schema).format("json").option("multiLine", "true").load(INPUT_PATH)

def add_columns(dataframe,colums):
    """
    Add new columns to a PySpark DataFrame by replacing dots with underscores in column names.

    Parameters:
    df (pyspark.sql.DataFrame): The input DataFrame.
    columns (list): A list of column names to add.

    Returns:
    pyspark.sql.DataFrame: The DataFrame with the new columns added.
    """
    for colm in colums:
        newvalue=colm.replace('.','_')
        dataframe=dataframe.withColumn(f'{newvalue}',col(colm))
    return dataframe

struct_explode_dict = con["intermediate_column_dict"]
array_explode_dict = con["array_column_dict"]
mappings=con["column_mapping"]
intermediate_target_schema=con["intermediate_target_schema"]
final_target_schema=con["final_target_schema"]

for i in range(1,(len(struct_explode_dict)+1)):
    df=add_columns(df,struct_explode_dict[f'{i}'])          
    for column in array_explode_dict[f'{i}']: 
        df = df.withColumn(f"new_{column}",explode_outer(column)).\
        drop(col(f"{column}")).withColumnRenamed(f'new_{column}',f'{column}')


#Mappings
for j in range(1, int(len(mappings)/2)+1):
    rule = mappings[f"Mapping_rule{j}"]
    new_col= mappings[f'column_name{j}']
    source = rule["source_column"]
    match = rule["match_value"]
    target = rule["target_column"]
    try:
        if new_col not in final_target_schema and new_col not in intermediate_target_schema:
            raise ValueError(f"{new_col} not present in either final or intermediate target schema")        
        if new_col in intermediate_target_schema:
            if source not in df.columns:
                raise ValueError(f"{source} column not present in the input dataframe")
            df = df.withColumn(new_col,(df[target]))
        else:
            if source not in df.columns or target not in df.columns:
                raise ValueError(f"{source} column or {target} column not present in the input dataframe")
            df = df.withColumn(new_col, when((df[source] == match), (df[target])))            
    except ValueError as e:
        print(f"Error: {source} or {target} of column {j} not present in dataframe")       

df=df.select(*final_target_schema).distinct()

#Output_Path=(org.json).split('.')[0]
df.write.mode("overwrite").parquet('/workspaces/FHIR_Framework/Output/org_aff')
df = pd.read_parquet('/workspaces/FHIR_Framework/Output/org_aff')
df.to_csv('file.csv', "\n")