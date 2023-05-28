import sys
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
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
    with open(f'D:/FHIR/Config_files/org_aff_config.json', encoding='utf-8') \
        as config_file:
        con = json.load(config_file)
except FileNotFoundError:
    print("Error: Config file not found")
    sys.exit(1)

schema_temp = con["Source_Schema"]
schema = StructType.fromJson(schema_temp)
INPUT_PATH = 'D:/FHIR/Input_data_files/org_aff.json'
df = spark.read.schema(schema).format("json").option("multiLine", "true").load(INPUT_PATH)


def add_columns(dataframe, columns):
    """
    Add new columns to a PySpark DataFrame by replacing dots with underscores in column names.

    Parameters:
    dataframe (pyspark.sql.DataFrame): The input DataFrame.
    columns (list): A list of column names to add.

    Returns:
    pyspark.sql.DataFrame: The DataFrame with the new columns added.
    """
    for colm in columns:
        new_value = colm.replace('.', '_')
        dataframe = dataframe.withColumn(f'{new_value}', col(colm))
    return dataframe

struct_explode_dict = con["intermediate_column_dict"]
array_explode_dict = con["array_column_dict"]
mappings = con["column_mapping"]
intermediate_target_schema = con["intermediate_target_schema"]
final_target_schema = con["final_target_schema"]

for i in range(1, len(struct_explode_dict) + 1):
    df = add_columns(df, struct_explode_dict[str(i)])
    for column in array_explode_dict[str(i)]:
        df = df.withColumn(f"{column}", explode_outer(column))

#column names in mappings
new_col_names = [mappings[key] for index, key in enumerate(mappings.keys()) if index%2==0]
#final_target_schema = list[final_target_schema]

# if len(new_col_names) !=len(final_target_schema) :
#     print("No match")
#     sys.exit(1)
   
# for j in range(1, int(len(mappings) / 2) + 1):
#     rule = mappings[f"Mapping_rule{j}"]
#     target = rule["target_column"]
#     new_col = mappings[f'column_name{j}']   
#     if 'target_column' in rule and 'match_value' not in rule and 'source_column' not in rule:
#         if target in df.columns:
#             df = df.withColumn(new_col, df[target])
#         continue

#     source = rule["source_column"]
#     match = rule["match_value"]

#     try:
#         if new_col not in final_target_schema and new_col not in intermediate_target_schema:
#             raise ValueError(f"{new_col} not present in either final or intermediate target schema")
#         else:
#             if source not in df.columns or target not in df.columns:
#                 df = df.withColumn(new_col, lit(None).cast(StringType()))
#                 raise ValueError(f"{source} column or {target} column not present in the input \
#                     dataframe so we created a null value for {new_col} ")
#             df = df.withColumn(new_col, when(df[source] == match, df[target]))
#     except ValueError as e:
#         print(f"Error: {source} or {target} of column_name {j} not present in dataframe \
#                 so we created a null value for {new_col}")

# df = df.select(*final_target_schema).distinct()

df.show()
print(df.count())

df.write.mode("overwrite").parquet('/workspaces/FHIR_Framework/Output/org_aff')
#Output_Path=(org.json).split('.')[0]
df = pd.read_parquet('/workspaces/FHIR_Framework/Output/org_aff')
df.to_csv('file.csv', "\n")