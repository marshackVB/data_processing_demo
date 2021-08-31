# Databricks notebook source
import pandas as pd

# COMMAND ----------

 df = pd.read_csv('/dbfs/FileStore/mlc/car_insurance_claim.csv')

# COMMAND ----------

demographics_cols = ['ID', 'BIRTH', 'AGE', 'HOMEKIDS', 'YOJ', 'INCOME', 'PARENT1', 'HOME_VAL', 'MSTATUS', 'GENDER', 'EDUCATION', 'OCCUPATION', 'URBANICITY']
policy_cols = ['ID', 'TRAVTIME', 'CAR_USE', 'BLUEBOOK', 'TIF', 'CAR_TYPE', 'RED_CAR', 'OLDCLAIM', 'CLM_FREQ', 'REVOKED', 'MVR_PTS', 'CLM_AMT', 'CAR_AGE', 'CLAIM_FLAG']

demographics_df = df[demographics_cols]
policy_df = df[policy_cols]

# COMMAND ----------

policy_df.head()

# COMMAND ----------

demographics_df.to_csv('/dbfs/FileStore/mlc/car_insurance_claim_demographics.csv', index=False)
policy_df.to_csv('/dbfs/FileStore/mlc/car_insurance_claim_policy.csv', index=False)

# COMMAND ----------

df.head()
