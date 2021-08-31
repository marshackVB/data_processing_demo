# Databricks notebook source
import pyspark.sql.functions as func
from pyspark.sql.functions import col

# COMMAND ----------

def replace_special_characters(df, column_name_and_transformation):
  """Given a dataframe, and column to regex expression dictionary, apply
  the regex expression to each column. Return a Spark DataFrame"""
  
  # Regex expressions
  transformations = {"dollar": "\\\$|,",
                     "z_pr": "z_"}
  
  all_columns = df.columns
  
  # Associate column name with regex
  column_name_and_regex = {column: transformations[column_name_and_transformation[column]] if column in column_name_and_transformation.keys() 
                           else 'none' for column in all_columns}
  
  regex = [f"regexp_replace({column}, '{regex_logic}', '') as {column}" if regex_logic != "none" 
           else column for column, regex_logic in column_name_and_regex.items()]
  
  # Perform transformation
  transformed_df = df.selectExpr(regex)
  
  return transformed_df
