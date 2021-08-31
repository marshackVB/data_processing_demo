# Databricks notebook source
# MAGIC %md ### Assign access for silver and bronze tables

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC GRANT SELECT ON default.mlc_policy_attributes TO DataProcessingDemoAccess;
# MAGIC GRANT SELECT ON default.mlc_demographic_attributes TO DataProcessingDemoAccess;
# MAGIC GRANT SELECT ON default.mlc_demographic_policy_gold TO DataProcessingDemoAccess;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW GRANT ON default.mlc_policy_attributes
