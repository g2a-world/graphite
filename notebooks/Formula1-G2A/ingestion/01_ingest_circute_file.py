# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest Circute.csv File

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

circuit_schema = StructType(fields = [StructField("circuitId", IntegerType(), False),
                                      StructField("circuitRef", StringType(), True),
                                      StructField("name", StringType(), True),
                                      StructField("location", StringType(), True),
                                      StructField("country", StringType(), True),
                                      StructField("lat", DoubleType(), True),
                                      StructField("lng", DoubleType(), True),
                                      StructField("alt", IntegerType(), True),
                                      StructField("url", StringType(), True)
                                    ]
                            )


# COMMAND ----------

circuits_df = spark.read \
    .option("header", True) \
    .schema(circuit_schema) \
    .csv("dbfs:/mnt/formula1dlg2a/raw/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2 - Select only the required columns

# COMMAND ----------

circuit_selected_df = circuits_df.select(col('circuitId'), col('circuitRef'), col('name'), col('location'), col('country')
                                         , col('lat'), col('lng'),  col('alt'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Renaming columns

# COMMAND ----------

circuit_renamed_df = circuit_selected_df.withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude")

# COMMAND ----------

# MAGIC %md
# MAGIC ######Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

circuit_final_df = circuit_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 5 - Write the final data to ADLS storage

# COMMAND ----------

circuit_final_df.write.mode("overwrite").parquet("/mnt/formula1dlg2a/processed/circuits")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlg2a/processed/circuits"))