# Databricks notebook source
# MAGIC %md ### Exploratory analysis of raw data

# COMMAND ----------

import pyspark.sql.functions as func
from pyspark.sql.functions import col

# COMMAND ----------

raw_data = spark.read.csv('dbfs:/FileStore/mlc/car_insurance_claim_policy.csv', header=True)
display(raw_data)

# COMMAND ----------

# MAGIC %md #### What is the distribution of insured vehicles by type and usage?

# COMMAND ----------

raw_data.createOrReplaceTempView('policy_attributes')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT CAR_TYPE as type,
# MAGIC        CAR_USE as usage,
# MAGIC        count(*) as count
# MAGIC FROM policy_attributes
# MAGIC GROUP BY CAR_TYPE, CAR_USE
# MAGIC ORDER BY CAR_TYPE, CAR_USE

# COMMAND ----------

# MAGIC %md #### What is the distribution of insured vehicles by type and usage?

# COMMAND ----------

travel_time = (spark.table('policy_attributes').withColumn('travel_time_bin', func.floor(col('TRAVTIME')/func.lit(10)))
                    .select('TRAVTIME', 'travel_time_bin'))

display(travel_time)

# COMMAND ----------

travel_time_grouped = (travel_time.groupBy('travel_time_bin').count()
                                  .orderBy(col('travel_time_bin')))
                      
display(travel_time_grouped)

# COMMAND ----------

# MAGIC %md #### Databricks operates via "lazy evaluation"

# COMMAND ----------


