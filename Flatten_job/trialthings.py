# def avoid_replication(hcs_df,cols_to_drop):
#     df1=hcs_df.select(cols_to_drop)
#     hcs_df=hcs_df.drop(*cols_to_drop)
#     df1 = df1.withColumn('temp_id', monotonically_increasing_id())
#     hcs_df = hcs_df.withColumn('temp_id', monotonically_increasing_id())
#     temp_df = df1.join(hcs_df, 'temp_id', 'outer').drop('temp_id')
#     # olread_exploded = [col_name for col_name, data_type in temp_df.dtypes \
#     #                    if "struct" in data_type or "array" in data_type]
#     # temp_df = temp_df.drop(*olread_exploded)
#     return temp_df

# def ifSameColumn():
#     {
#        print("checking") 
#     }


# final_df=avoid_replication(hcs_df,cols_to_drop)
# final_df.show()
# print(final_df.count())

# cols_to_drop=hcs_df.columns