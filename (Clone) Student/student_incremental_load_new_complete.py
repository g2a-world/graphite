# Databricks notebook source
# Import necessary functions for working with timestamps
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType
from delta.tables import DeltaTable

# Define Delta table paths
# source_table_path = "/mnt/formula1dlg2a/presentation/student_global/std_parquet"
source_table_path = "/mnt/formula1dlg2a/presentation/student_global/std_bulk_record"
destination_table_path = "/mnt/formula1dlg2a/presentation/student_ifp"

# Perform transformations and join operations
# source_data = spark.read.option("inferSchema", True) \
#         .parquet(source_table_path) \
#             .withColumn("UpdatedDate", lit(None)) \
#                 .withColumn("CreatedDate", current_timestamp())

source_data = spark.read \
    .format("delta") \
        .load(source_table_path) \
            .withColumn("UpdatedDate", lit(None)) \
                .withColumn("CreatedDate", current_timestamp())

# Check if the previous day's result Delta table exists
if DeltaTable.isDeltaTable(spark, f"{destination_table_path}/summary_table"):
    previous_result = DeltaTable.forPath(spark, f"{destination_table_path}/summary_table")

        # Check if the UpdatedDate column exists in the schema
    if "UpdatedDate" not in previous_result.toDF().columns:
        # If not, add it with a default value or set it to null
        previous_result.toDF().withColumn("UpdatedDate", current_timestamp()).write.format("delta") \
            .option("mergeSchema", "true") \
                .mode("overwrite").save(f"{destination_table_path}/summary_table")
        previous_result = DeltaTable.forPath(spark, f"{destination_table_path}/summary_table")

else:
    # If Delta table doesn't exist, initialize it with new_updated_records
    source_data.write.format("delta").mode("overwrite").save(f"{destination_table_path}/summary_table")
    previous_result = DeltaTable.forPath(spark, f"{destination_table_path}/summary_table")

# Identify New or Updated Records
new_delta_records = source_data.select('ID', 'FirstName', 'LastName', 'BirthDate', 'Departiment', 'Sallary').subtract(previous_result.toDF().select('ID', 'FirstName', 'LastName', 'BirthDate', 'Departiment', 'Sallary'))

# Perform the merge operation
if not new_delta_records.isEmpty():
    # Perform the merge operation
    previous_result.alias("target").merge(
        new_delta_records.alias("source"), "target.ID = source.ID"
    ) \
        .whenMatchedUpdate(set={
            "FirstName": col("source.FirstName"),
            "LastName": col("source.LastName"),
            "BirthDate": col("source.BirthDate"),
            "Departiment": col("source.Departiment"),
            "Sallary": col("source.Sallary"),
            "UpdatedDate": current_timestamp(),
        }) \
        .whenNotMatchedInsert(values={
            "ID": col("source.ID"),
            "FirstName": col("source.FirstName"),
            "LastName": col("source.LastName"),
            "BirthDate": col("source.BirthDate"),
            "Departiment": col("source.Departiment"),
            "Sallary": col("source.Sallary"),
            "CreatedDate": current_timestamp(),
            "UpdatedDate": current_timestamp(),
        }) \
        .execute()

# Final Write to Delta Table
# Final Write to Delta Table
temp_df = spark.read.format("delta").load(f"{destination_table_path}/summary_table")
temp_df.write \
    .format("delta") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
            .saveAsTable("ifp_student_db.student_record")


# COMMAND ----------



# COMMAND ----------

# from pyspark.sql.functions import col, current_timestamp, lit
# from delta.tables import DeltaTable

# # Define Delta table paths
# source_table_path = "/mnt/formula1dlg2a/presentation/student_global/std_bulk_record"
# destination_table_path = "/mnt/formula1dlg2a/presentation/student_ifp"

# # Infer schema from the Delta table for source_data
# source_data = spark.read.format("delta").load(source_table_path)

# # Define audit column names
# created_date_col = "CreatedDate"
# updated_date_col = "UpdatedDate"

# # Add audit columns with default values
# source_data = source_data.withColumn(updated_date_col, lit(None)).withColumn(created_date_col, current_timestamp())

# # Check if the previous day's result Delta table exists
# delta_table_path = f"{destination_table_path}/summary_table"
# if DeltaTable.isDeltaTable(spark, delta_table_path):
#     previous_result = DeltaTable.forPath(spark, delta_table_path)

#     # Check if the UpdatedDate column exists in the schema
#     if updated_date_col not in previous_result.toDF().columns:
#         # If not, add it with a default value or set it to null
#         previous_result.toDF().withColumn(updated_date_col, current_timestamp()).write.format("delta") \
#             .option("mergeSchema", "true") \
#             .mode("overwrite").save(delta_table_path)
#         previous_result = DeltaTable.forPath(spark, delta_table_path)
# else:
#     # If Delta table doesn't exist, initialize it with new_updated_records
#     source_data.write.format("delta").mode("overwrite").save(delta_table_path)
#     previous_result = DeltaTable.forPath(spark, delta_table_path)

# # Identify New or Updated Records
# source_columns = ['ID', 'FirstName', 'LastName', 'BirthDate', 'Departiment', 'Sallary']
# new_delta_records = source_data.select(*source_columns).subtract(previous_result.toDF().select(*source_columns))

# # Perform the merge operation
# if not new_delta_records.isEmpty():
#     previous_result.alias("target").merge(
#         new_delta_records.alias("source"), "target.ID = source.ID"
#     ) \
#         .whenMatchedUpdate(set={
#             "FirstName": col("source.FirstName"),
#             "LastName": col("source.LastName"),
#             "BirthDate": col("source.BirthDate"),
#             "Departiment": col("source.Departiment"),
#             "Sallary": col("source.Sallary"),
#             updated_date_col: current_timestamp(),
#         }) \
#         .whenNotMatchedInsert(values={
#             "ID": col("source.ID"),
#             "FirstName": col("source.FirstName"),
#             "LastName": col("source.LastName"),
#             "BirthDate": col("source.BirthDate"),
#             "Departiment": col("source.Departiment"),
#             "Sallary": col("source.Sallary"),
#             created_date_col: current_timestamp(),
#             updated_date_col: current_timestamp(),
#         }) \
#         .execute()

# # Final Write to Delta Table
# temp_df = spark.read.format("delta").load(delta_table_path)
# temp_df.write \
#     .format("delta") \
#     .option("mergeSchema", "true") \
#     .mode("overwrite") \
#     .saveAsTable("ifp_student_db.student_record")

