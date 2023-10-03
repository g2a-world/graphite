-- Databricks notebook source
-- CREATE DATABASE ifp_student_db
-- Location "/mnt/formula1dlg2a/presentation/student_ifp/"

-- COMMAND ----------

-- CREATE DATABASE global_student_db
-- Location "/mnt/formula1dlg2a/presentation/student_global/"

-- COMMAND ----------

-- Create table If Not Exists ifp_student_db.student_record  (
--   ID integer,
--   FirstName string,
--   LastName string,
--   BirthDate date,
--   Departiment string,
--   Sallary integer,
--   CreatedDate timestamp,
--   UpdatedDate timestamp
-- )
-- Using DELTA

-- COMMAND ----------

Select * From global_student_db.std_bulk_record

-- COMMAND ----------

-- DESC HISTORY global_student_db.std_bulk_record

-- COMMAND ----------

Select * From global_student_db.std_parquet

-- COMMAND ----------



-- COMMAND ----------

Select * from ifp_student_db.student_record 

-- COMMAND ----------

DESC HISTORY ifp_student_db.student_record

-- COMMAND ----------

-- drop table ifp_student_db.student_record

-- COMMAND ----------

-- drop table global_student_db.std_bulk_record

-- COMMAND ----------


