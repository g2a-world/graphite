# Databricks notebook source
from datetime import datetime
from delta.tables import DeltaTable
import json
from pyspark.sql.functions import *

# COMMAND ----------



# Perform transformations and join operations
new_updated_records = spark.read \
    .format("delta") \
        .load("/mnt/formula1dlg2a/presentation/student_global/std_bulk_record")

# Check if the previous day's result Delta table exists
previous_result_path = f"/mnt/formula1dlg2a/presentation/student_ifp/student_record/summary_table"

if DeltaTable.isDeltaTable(spark, previous_result_path):
    previous_result = DeltaTable.forPath(spark, previous_result_path)

else:
    # If Delta table doesn't exist, initialize it with new_updated_records
    new_updated_records.write.format("delta").mode("overwrite").save(previous_result_path)
    previous_result = DeltaTable.forPath(spark, previous_result_path)

# Identify New or Updated Records
new_delta_records = new_updated_records.subtract(previous_result.toDF())

# Perform the merge operation
if not new_delta_records.isEmpty():
    previous_result.alias("target").merge(
        new_delta_records.alias("source"), "target.ID = source.ID") \
            .whenMatchedUpdateAll(set {"FirstName" : "source.FirstName", "LastName" : "source.LastName", "BirthDate" : "source.BirthDate", "Departiment" : "source.Departiment",
                                       "Sallary" : "source.Sallary", "UpdatedDate" : "current_timestamp()"}) \
                .whenNotMatchedInsertAll(values = {
                    "ID" : "source.ID",
                    "FirstName" : "source.FirstName", 
                    "LastName" : "source.LastName", 
                    "BirthDate" : "source.BirthDate", 
                    "Departiment" : "source.Departiment",
                    "Sallary" : "source.Sallary", 
                    "CreatedDate" : "current_timestamp()"
                }) \
                .execute()
 
temp_df = spark.read.format("delta").load(previous_result_path)
temp_df.write.format("delta").mode("overwrite").saveAsTable("ifp_student_db.student_record")

# # updated capture the new last_run_date in the config file
# # After successful processing, update the timestamp and write it back to the file
# # Generate the new timestamp based on the current time


# COMMAND ----------

temp_df = spark.read.format("delta").load(previous_result_path)

display(temp_df)

# COMMAND ----------

display(new_updated_records)

# COMMAND ----------

display(new_delta_records)

# COMMAND ----------

display(spark.read.format("delta").load("/mnt/formula1dlg2a/presentation/student_global/std_bulk_record"))

# COMMAND ----------


# # Read the configuration file
# with open("/dbfs/mnt/formula1dlg2a/student/config.json", "r") as config_file:
#     config = json.load(config_file)

# # Get the last run timestamp
# last_run_timestamp = config.get("last_run_timestamp")

# # Parse the timestamp string into a datetime object
# last_run_datetime = datetime.strptime(last_run_timestamp, "%Y-%m-%dT%H:%M:%S.%f")

# # Convert last_run_datetime to a timestamp string in the expected format
# timestamp_str = last_run_datetime.strftime("%Y-%m-%dT%H:%M:%S")

# # Perform transformations and join operations
# result = spark.read \
#     .format("delta") \
#         .option("timestampAsOf", last_run_datetime) \
#             .load("/mnt/formula1dlg2a/presentation/student_global/std_bulk_record")

# # Check if the previous day's result Delta table exists
# previous_result_path = f"/mnt/formula1dlg2a/presentation/student_ifp/student_record/summary_table"
# if DeltaTable.isDeltaTable(spark, previous_result_path):
#     previous_result = DeltaTable.forPath(spark, previous_result_path)
# else:
#     # Create an empty DataFrame if the Delta table doesn't exist
#     previous_result = None

# if previous_result:
#     # Identify new or updated records by comparing with the previous day's data
#     new_updated_records = result.subtract(previous_result.toDF())
# # ----------------------------------------------------------------------------------------
#     # # Define the merge condition and update logic
#     # merge_condition = "target.ID = source.ID"
#     # update_logic = {
#     #                 "target.FirstName = source.FirstName",
#     #                 "target.LastName = source.LastName",
#     #                 "target.BirthDate = source.BirthDate",
#     #                 "target.Departiment = source.Departiment",
#     #                 "target.Sallary = source.Sallary"
#     #                 # Add more columns as needed
#     # }
# # --------------------------------------------------------------------------------------------------
#     # Perform the merge operation

#     previous_result.alias("target").merge(
#         new_updated_records.alias("source"), "target.ID = source.ID") \
#             .whenMatchedUpdateAll() \
#                 .whenNotMatchedInsertAll() \
#                     .execute()
#             # .whenMatchedUpdate(set=update_logic).execute()
    
# else:
#     # If previous_result is None, simply write the result as the initial Delta table
#     result.write.format("delta").mode("overwrite").save(previous_result_path)


# temp_df = spark.read.format("delta").load(new_result_path)
# temp_df.write.format("delta").mode("overwrite").saveAsTable("ifp_student_db.student_record")

# # # Example usage with a specific timestamp
# # last_run_timestamp = datetime(2023, 9, 15)  # Use your desired timestamp here
# # process_with_timestamp(last_run_timestamp)

# # # updated capture the new last_run_date in the config file
# # # After successful processing, update the timestamp and write it back to the file
# # # Generate the new timestamp based on the current time
# new_timestamp = datetime.utcnow().isoformat()
# config["last_run_timestamp"] = new_timestamp

# with open("/dbfs/mnt/formula1dlg2a/student/config.json", "w") as config_file:
#     json.dump(config, config_file, indent=2)

