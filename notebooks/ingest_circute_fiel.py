# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pyspark.sql.types import *
from pyspark.sql.functions import *

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

circuits_df = spark.read \
    .option("header", True) \
    .schema(circuit_schema) \
    .csv("dbfs:/mnt/formula1dlg2a/raw/circuits.csv")

circuit_selected_df = circuits_df.select(col('circuitId'), col('circuitRef'), col('name'), col('location'), col('country')
                                         , col('lat'), col('lng'),  col('alt'))

circuit_renamed_df = circuit_selected_df.withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude")

circuit_final_df = circuit_renamed_df.withColumn("ingestion_date", current_timestamp())

circuit_final_df.write.mode("overwrite").parquet("/mnt/formula1dlg2a/processed/circuits")

display(spark.read.parquet("/mnt/formula1dlg2a/processed/circuits"))

