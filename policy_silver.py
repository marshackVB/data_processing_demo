# Databricks notebook source
# MAGIC %md ## Data transformation logic for policy attributes table

# COMMAND ----------

import pyspark.sql.functions as func
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md Import data processing functions

# COMMAND ----------

# MAGIC %run ./transformation_functions

# COMMAND ----------

# MAGIC %md Read in raw data

# COMMAND ----------

raw_data = spark.read.csv('dbfs:/FileStore/mlc/car_insurance_claim_policy.csv', header=True)

display(raw_data)

# COMMAND ----------

# MAGIC %md Apply transformations

# COMMAND ----------

column_name_and_transformation = {"BLUEBOOK": "dollar",
                                  "OLDCLAIM": "dollar",
                                  "CLM_AMT":  "dollar",
                                  "CAR_TYPE": "z_pr"}

transformed_df = replace_special_characters(raw_data, column_name_and_transformation)

display(transformed_df)

# COMMAND ----------

# MAGIC %md Write to Delta table

# COMMAND ----------

transformed_df.write.mode('append').format('delta').saveAsTable('default.mlc_policy_attributes')
