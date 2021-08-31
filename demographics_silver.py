# Databricks notebook source
# MAGIC %md ## Data transformation logic for demographic attributes table

# COMMAND ----------

raw_data = spark.read.csv('dbfs:/FileStore/mlc/car_insurance_claim_demographics.csv', header=True)
raw_data.createOrReplaceTempView('raw_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM raw_data
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW transformed_df AS
# MAGIC SELECT ID,
# MAGIC        BIRTH,
# MAGIC        AGE,
# MAGIC        HOMEKIDS,
# MAGIC        YOJ,
# MAGIC        cast(regexp_replace(INCOME, "\\\$|,", "") as DOUBLE) as INCOME,
# MAGIC        PARENT1,
# MAGIC        cast(regexp_replace(HOME_VAL, "\\\$|,", "") as DOUBLE) as HOME_VAL,
# MAGIC        regexp_replace(MSTATUS, "z_", "") as MSTATUS,
# MAGIC        regexp_replace(GENDER, "z_", "") as GENDER,
# MAGIC        regexp_replace(OCCUPATION, "z_", "") as OCCUPATION,
# MAGIC        URBANICITY
# MAGIC FROM raw_data

# COMMAND ----------

spark.table('transformed_df').write.mode('append').format('delta').saveAsTable('default.mlc_demographic_attributes')
