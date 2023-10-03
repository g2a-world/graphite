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

    # Left join source_data with previous_result using "ID" as the key
    joined_data = source_data.alias("source").join(
        previous_result.toDF().alias("target"),
        col("source.ID") == col("target.ID"), "left")
else:
    # Create an empty DataFrame if the Delta table doesn't exist
    previous_result = None

# COMMAND ----------

display(joined_data)

# COMMAND ----------

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

    # Left join source_data with previous_result using "ID" as the key
    joined_data = source_data.alias("s").join(
        previous_result.toDF().alias("t"),
        col("s.ID") == col("t.ID"), "left")
else:
    # Create an empty DataFrame if the Delta table doesn't exist
    previous_result = None

if previous_result:
    # Filter for new or updated records where there is no match in the target table
    new_updated_records_1 = joined_data.filter("t.ID is null")
    new_updated_records = new_updated_records_1.select("s.ID", "s.FirstName", "s.LastName", "s.BirthDate", "s.Departiment", "s.Sallary")


# if previous_result:
#     # Identify new or updated records by comparing with the previous day's data
#     new_updated_records = source_data.select(
#         "ID", "FirstName", "LastName", "BirthDate", "Departiment", "Sallary"
#     ).subtract(previous_result.toDF())

#     # Add the new audit columns with default values
#     new_updated_records = new_updated_records.withColumn("created_date", lit(None).cast("timestamp")) \
#         .withColumn("updated_date", current_timestamp())

# # if previous_result:
# #     # Identify new or updated records by comparing with the previous day's data
# #     # new_updated_records = source_data.subtract(previous_result.toDF())

# ----------------------------------------------------------------------------------------
    # Perform the merge operation
    previous_result.alias("target").merge(
    new_updated_records.alias("source"), "target.ID = source.ID") \
        .whenMatchedUpdate(set={
            "FirstName": col("source.FirstName"),
            "LastName": col("source.LastName"),
            "BirthDate": col("source.BirthDate"),
            "Departiment": col("source.Departiment"),
            "Sallary": col("source.Sallary"),
            "UpdatedDate": current_timestamp()
        }) \
        .whenNotMatchedInsert(values = {
            "ID" : col("source.ID"),
            "FirstName" : col("source.FirstName"), 
            "LastName" : col("source.LastName"), 
            "BirthDate" : col("source.BirthDate"), 
            "Departiment" : col("source.Departiment"),
            "Sallary" : col("source.Sallary"), 
            "CreatedDate" : current_timestamp()
        }) \
        .execute()

    # previous_result.alias("target").merge(
    #     new_updated_records.alias("source"), "target.ID = source.ID") \
    #         .whenMatchedUpdate(set={
    #             "FirstName": "source.FirstName",
    #             "LastName": "source.LastName",
    #             "BirthDate": "source.BirthDate",
    #             "Departiment": "source.Departiment",
    #             "Sallary": "source.Sallary",
    #             "UpdatedDate": "current_timestamp()"
    #         }) \
    #         .whenNotMatchedInsert(values = {
    #             "ID" : "source.ID",
    #             "FirstName" : "source.FirstName", 
    #             "LastName" : "source.LastName", 
    #             "BirthDate" : "source.BirthDate", 
    #             "Departiment" : "source.Departiment",
    #             "Sallary" : "source.Sallary", 
    #             "CreatedDate" : "current_timestamp()"
    #         }) \
    #         .execute()

else:
    # If previous_result is None, simply write the result as the initial Delta table
    source_data_with_audit = source_data.withColumn("updated_date", lit(None).cast("timestamp")) \
        .withColumn("created_date", current_timestamp())
    source_data_with_audit.write.format("delta").mode("overwrite").save(destination_table_path)

# Final Write to Delta Table
temp_df = spark.read.format("delta").load(destination_table_path)
temp_df.write.format("delta").mode("overwrite").saveAsTable("ifp_student_db.student_record")


# COMMAND ----------

display(new_updated_records)

# COMMAND ----------


