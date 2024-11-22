# Databricks notebook source
# Function to get available versions using DESCRIBE HISTORY
def get_table_versions(table_path):
    history_df = spark.sql(f"DESCRIBE HISTORY delta.{table_path}")
    history_df.select("version", "timestamp", "operation").show(truncate=False)
    return history_df

# Function to query the Delta table for a specific version
def query_table_by_version(table_path, version):
    query = f"SELECT * FROM delta.{table_path} VERSION AS OF {version}"
    versioned_data = spark.sql(query)
    return versioned_data



# COMMAND ----------

# Specifying the Delta table path
table_path = "/mnt/delta/your_table"

# Fetching available versions 
get_table_versions(table_path)

# Example usage: specify the version to query
selected_version = 2 

# Querying the table for the selected version
versioned_data = query_table_by_version(table_path, selected_version)

# Showing the queried data
versioned_data.show(truncate=False)
