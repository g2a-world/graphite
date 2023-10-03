# Databricks notebook source
import json
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from datetime import datetime

# COMMAND ----------

# Read the configuration file
with open("/dbfs/mnt/formula1dlg2a/student/config.json", "r") as config_file:
    config = json.load(config_file)

# Get the last run timestamp
last_run_timestamp = config.get("last_run_timestamp")

# # updated capture the new last_run_date in the config file
# # After successful processing, update the timestamp and write it back to the file
# # Generate the new timestamp based on the current time
new_timestamp = datetime.utcnow().isoformat()
config["last_run_timestamp"] = new_timestamp

with open("/dbfs/mnt/formula1dlg2a/student/config.json", "w") as config_file:
    json.dump(config, config_file, indent=2)

# COMMAND ----------


# Read existing data at the source location
source_table = spark.read \
    .format("delta") \
        .load("/mnt/formula1dlg2a/presentation/student_global/std_bulk_record")

# # Define the path to your Delta table
# delta_table_path = "/mnt/formula1dlg2a/presentation/student_global/std_bulk_record"

# # Create a DeltaTable instance
# delta_table = DeltaTable.forPath(spark, delta_table_path)

# filter only new or updated records since the last successfull run
source_changes_df = spark.read.format("delta").option("timestampAsOf", last_run_timestamp).load("/mnt/formula1dlg2a/presentation/student_global/std_bulk_record")

# source_changes_df = source_table.history().filter(col("timestamp") > last_run_timestamp)
# .select("operation", "path")




# COMMAND ----------

changes_df = delta_table.history().filter(col("timestamp") > last_run_timestamp)
# .select("WRITE", "/mnt/formula1dlg2a/presentation/student_global/std_bulk_record")

# COMMAND ----------

display(changes_df)

# COMMAND ----------

display(spark.read.format("delta").option("timestampAsOf", last_run_timestamp).load("/mnt/formula1dlg2a/presentation/student_global/std_bulk_record"))

# COMMAND ----------

if (spark._jsparkSession.catalog().tableExists("ifp_student_db.student_record")):
    deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dlg2a/presentation/student_ifp/student_record")
    deltaTable.alias("t").merge(source_changes_df.alias("s"), "t.ID = s.ID") \
        .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
                .execute()
else:
    source_changes_df.write.mode("append").format("delta").partitionBy("Departiment").option("path","/mnt/formula1dlg2a/presentation/student_ifp/student_record").saveAsTable("ifp_student_db.student_record")

# COMMAND ----------

# 2023-09-16T14:25:00Z
