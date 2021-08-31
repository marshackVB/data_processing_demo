# Databricks notebook source
# MAGIC %md #### Transormation logic for dempgraphic and policy golden-layer table

# COMMAND ----------

policy_data_silver = spark.table('default.mlc_policy_attributes')
demographic_data_silver = spark.table('default.mlc_demographic_attributes')

# COMMAND ----------

display(demographic_data_silver)

# COMMAND ----------

display(policy_data_silver)

# COMMAND ----------

final_table = policy_data_silver.join(demographic_data_silver, ['ID'], 'inner')

# COMMAND ----------

display(final_table)

# COMMAND ----------

final_table.write.mode('append').format('delta').saveAsTable('default.mlc_demographic_policy_gold')
