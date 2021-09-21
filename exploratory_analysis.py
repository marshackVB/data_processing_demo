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

travel_time = (spark.table('policy_attributes').withColumn('travel_time_bin', func.floor(col('TRAVTIME')/func.lit(10)) * 10)
                    .select('TRAVTIME', 'travel_time_bin'))

display(travel_time)

# COMMAND ----------

travel_time_grouped = (travel_time.groupBy('travel_time_bin').count()
                                  .orderBy(col('travel_time_bin')))
                      
display(travel_time_grouped)

# COMMAND ----------

# MAGIC %md #### Databricks operates via "lazy evaluation"

# COMMAND ----------

# Remove special characters from 
sql_expressions = ["CAR_USE",
                   "cast(regexp_replace(BLUEBOOK, '\\\$|,', '') as DOUBLE) as BLUEBOOK",
                   "cast(regexp_replace(OLDCLAIM, '\\\$|,', '') as DOUBLE) as OLDCLAIM",
                   "cast(regexp_replace(CLM_AMT, '\\\$|,', '') as DOUBLE) as CLM_AMT"]

transformation_1 = spark.table('policy_attributes').selectExpr(sql_expressions)

# Apply sequential application of business logic
transformation_2 = (transformation_1.filter(col('CLM_AMT') > 0)
                                    # Calculate difference between old and new claims
                                    .withColumn('new_claim_vs_old', col('CLM_AMT') - col('OLDCLAIM'))
                    
                                    # Retain only claims that have increased in liability
                                    .filter(col('new_claim_vs_old') > 0)
                    
                                    # Sum liability over car use type
                                    .groupBy('CAR_USE').agg(func.sum('new_claim_vs_old').alias('claim_amnt_increase'))
                                    .orderBy('claim_amnt_increase'))

# COMMAND ----------

display(transformation_2)

# COMMAND ----------

# MAGIC %md #### All the features of a generally purpose programming language are at your disposal

# COMMAND ----------

display(spark.table('policy_attributes').select('BLUEBOOK', 'OLDCLAIM', 'CLM_AMT', 'CAR_TYPE'))

# COMMAND ----------

# MAGIC %md A resuable function where a common problem can be solved only once

# COMMAND ----------

def replace_special_characters(df, column_name_and_transformation):
  """Given a dataframe, and column to regex expression dictionary, apply
  the regex expression to each column. Return a Spark DataFrame"""
  
  transformations = {"dollar": "\\\$|,",
                     "z_pr": "z_"}
  
  column_name_and_regex = {column: transformations[transformation_type] for column, transformation_type in column_name_and_transformation.items()}
  
  regex = [f"regexp_replace({column}, '{regex_logic}', '') as {column}" 
         for column, regex_logic in column_name_and_regex.items()]
  
  transformed_df = df.selectExpr(regex)
  
  return transformed_df

# COMMAND ----------

# Dictionary of column name to regex transformation
column_name_and_transformation = {"BLUEBOOK": "dollar",
                                  "OLDCLAIM": "dollar",
                                  "CLM_AMT":  "dollar",
                                  "CAR_TYPE": "z_pr"}

transformed_df = replace_special_characters(raw_data, column_name_and_transformation)

display(transformed_df)
