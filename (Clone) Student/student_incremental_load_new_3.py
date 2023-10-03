# Databricks notebook source
from datetime import datetime
from delta.tables import DeltaTable
import json
from pyspark.sql.functions import *

## Define Delta table paths
source_table_path = "/mnt/formula1dlg2a/presentation/student_global/std_bulk_record"
destination_table_path = "/mnt/formula1dlg2a/presentation/student_ifp/summary_table"

 # Perform transformations and join operations
source_data = spark.read \
    .format("delta") \
        .load(source_table_path)

# Check if the previous day's result Delta table exists

if DeltaTable.isDeltaTable(spark, destination_table_path):
    previous_result = DeltaTable.forPath(spark, destination_table_path)

else:
    # If Delta table doesn't exist, initialize it with new_updated_records
    source_data.write.format("delta").mode("overwrite").save(destination_table_path)
    previous_result = DeltaTable.forPath(spark, destination_table_path)

# Identify New or Updated Records
new_delta_records = source_data.subtract(previous_result.toDF())

# Perform the merge operation
if not new_delta_records.isEmpty():
    # Perform the merge operation
    previous_result.alias("target").merge(
    new_delta_records.alias("source"), "target.ID = source.ID") \
        .whenMatchedUpdate(set={
            "FirstName": col("source.FirstName"),
            "LastName": col("source.LastName"),
            "BirthDate": col("source.BirthDate"),
            "Departiment": col("source.Departiment"),
            "Sallary": col("source.Sallary")
            # "UpdatedDate": current_timestamp()
        }) \
        .whenNotMatchedInsert(values = {
            "ID" : col("source.ID"),
            "FirstName" : col("source.FirstName"), 
            "LastName" : col("source.LastName"), 
            "BirthDate" : col("source.BirthDate"), 
            "Departiment" : col("source.Departiment"),
            "Sallary" : col("source.Sallary")
            # "CreatedDate" : current_timestamp()
        }) \
        .execute()

# Final Write to Delta Table
temp_df = spark.read.format("delta").load(destination_table_path)
temp_df.write.format("delta").mode("overwrite").saveAsTable("ifp_student_db.student_record")



# COMMAND ----------

display(previous_result_df)

# COMMAND ----------

display(new_delta_records)

# COMMAND ----------

display(previous_result)

# COMMAND ----------

display(spark.read.format("delta").load("/mnt/formula1dlg2a/presentation/student_global/std_bulk_record"))
