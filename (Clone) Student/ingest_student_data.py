# Databricks notebook source
# dbfs:/mnt/formula1dlg2a/student/config.json
# dbfs:/mnt/formula1dlg2a/student/student_day_1.csv
# dbfs:/mnt/formula1dlg2a/student/student_day_2.csv
# dbfs:/mnt/formula1dlg2a/student/student_day_3.csv

# dbfs:/mnt/formula1dlg2a/presentation/student_global/
# dbfs:/mnt/formula1dlg2a/presentation/student_ifp/

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read \
    .option("inferSchema", True) \
        .option("header", True) \
            .csv("/mnt/formula1dlg2a/student/student_day_1.csv").withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

df.write.format("delta").mode("overwrite").option("path", "/mnt/formula1dlg2a/presentation/student_global/std_bulk_record").saveAsTable("global_student_db.std_bulk_record")

# COMMAND ----------

# df.write.format("parquet").mode("overwrite").option("path", "/mnt/formula1dlg2a/presentation/student_global/std_parquet").saveAsTable("global_student_db.std_parquet")

# COMMAND ----------

# df = spark.read.parquet("/mnt/formula1dlg2a/presentation/student_global/std_bulk_record")
# display(df)

# COMMAND ----------


