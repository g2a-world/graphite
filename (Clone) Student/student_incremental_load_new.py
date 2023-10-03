# Databricks notebook source
import json
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from datetime import datetime

# COMMAND ----------

# # Read the configuration file
# with open("/dbfs/mnt/formula1dlg2a/student/config.json", "r") as config_file:
#     config = json.load(config_file)

# # Get the last run timestamp
# last_run_timestamp = config.get("last_run_timestamp")

# # # updated capture the new last_run_date in the config file
# # # After successful processing, update the timestamp and write it back to the file
# # # Generate the new timestamp based on the current time
# new_timestamp = datetime.utcnow().isoformat()
# config["last_run_timestamp"] = new_timestamp

# with open("/dbfs/mnt/formula1dlg2a/student/config.json", "w") as config_file:
#     json.dump(config, config_file, indent=2)

# COMMAND ----------

# Read the configuration file
with open("/dbfs/mnt/formula1dlg2a/student/config.json", "r") as config_file:
    config = json.load(config_file)

# Get the last run timestamp
last_run_timestamp = config.get("last_run_timestamp")

result = spark.read \
.format("delta") \
    .load("/mnt/formula1dlg2a/presentation/student_global/std_bulk_record")

# Perform transformations and join operations
# result = table1.join(table2, "join_column").join(table3, "join_column").transformations()

# Check if the previous day's result Delta table exists
previous_result_path = f"/mnt/formula1dlg2a/presentation/student_ifp/student_record/day_{last_run_timestamp}_result"
if DeltaTable.isDeltaTable(spark, previous_result_path):
    previous_result = spark.read.format("delta").load(previous_result_path)
else:
    # Create an empty DataFrame if the Delta table doesn't exist
    previous_result = spark.createDataFrame([], result.schema)


# Identify new or updated records by comparing with the previous day's data
    new_updated_records = result.subtract(previous_result)

    # Write the new or updated records to the destination Delta table using merge
    if not new_updated_records.isEmpty():
        new_updated_records.write.format("delta").mode("append").save(previous_result_path)

# # Identify new or updated records by comparing with the previous day's data
# if previous_result:
#     new_updated_records = result.subtract(previous_result)
# else:
#     new_updated_records = result

# # Write the new or updated records to the destination Delta table using merge
# if not new_updated_records.isEmpty():
#     new_updated_records.write.format("delta").mode("append").save(f"/mnt/formula1dlg2a/presentation/student_ifp/student_record/day_{last_run_timestamp}_result")

# Example usage with a specific timestamp
# last_run_timestamp = datetime(2023, 9, 15)  # Use your desired timestamp here
# process_with_timestamp(last_run_timestamp)



# # updated capture the new last_run_date in the config file
# # After successful processing, update the timestamp and write it back to the file
# # Generate the new timestamp based on the current time
new_timestamp = datetime.utcnow().isoformat()
config["last_run_timestamp"] = new_timestamp

with open("/dbfs/mnt/formula1dlg2a/student/config.json", "w") as config_file:
    json.dump(config, config_file, indent=2)


# COMMAND ----------

# display(last_run_timestamp)

# COMMAND ----------

# process_with_timestamp('2023, 09, 16')

# COMMAND ----------


