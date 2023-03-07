import pandas as pd
import json

Input_File = input("Enter the name of the input file or folder: ")
Config_File = input("Enter the name of the config file or folder: ")

# Load the configuration file
with open(f'/workspaces/FHIR_Framework/Config_files/{Config_File}') as config_file:
    con = json.load(config_file)

# Load the input JSON file into a Pandas dataframe
input_path = f'/workspaces/FHIR_Framework/Input_data_files/{Input_File}'
df = pd.read_json(input_path)

# Add new columns to the dataframe
struct_explode_dict = con["intermediate_column_dict"]
array_explode_dict = con["array_column_dict"]

for i in range(1, len(struct_explode_dict) + 1):
    for colm in struct_explode_dict[f"{i}"]:
        newvalue = colm.replace(".", "_")
        df[newvalue] = df[colm]

    for column in array_explode_dict[f"{i}"]:
        s = df[column].apply(pd.Series).stack().reset_index(level=1, drop=True)
        s.name = f"{column}_"
        df = df.drop(column, axis=1).join(s)

# Select the final target columns
final_target_schema = con["final_target_schema"]
df = df[final_target_schema]

# Save the output as a Parquet file
output_path = Input_File.split(".")[0]
df.to_parquet(f"/workspaces/FHIR_Framework/Output/{output_path}", engine="fastparquet")
