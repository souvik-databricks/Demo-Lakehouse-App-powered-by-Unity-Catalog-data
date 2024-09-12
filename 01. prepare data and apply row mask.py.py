# Databricks notebook source
# MAGIC %md
# MAGIC # Setting up the Unity Catalog masked data
# MAGIC
# MAGIC As part of the demo, we would be using some sample data from databricks and preparing our app's backend data. After preparing the data, we will be `applying  row masking on the data using Unity Catalog (UC).`
# MAGIC
# MAGIC We will be doing the following steps:
# MAGIC
# MAGIC >   1. Load the common configs for the table name and schema name.
# MAGIC >   2. Read `customer` and `nation` datasets from the databricks samples datasets.
# MAGIC >   3. Join those two datasets to prepare the data for the demo and save it as a delta table in UC.
# MAGIC >   4. Define the row masking function and save the function in UC.
# MAGIC >   5. Apply the row masking on the prepared dataset.
# MAGIC
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loading the configurations

# COMMAND ----------

# MAGIC %run ./configs.py

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading the sample datasets

# COMMAND ----------

customer = spark.table("samples.tpch.customer").selectExpr(
    "c_custkey as customer_id",
    "c_nationkey as nation_key",
    "c_mktsegment as market_segment",
)
nation = spark.table("samples.tpch.nation").selectExpr(
    "n_nationkey as nation_key", "n_name as nation"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparing the app's dataset

# COMMAND ----------

demo_dataset = customer.join(
    nation, customer.nation_key == nation.nation_key, "inner"
).selectExpr("customer_id", "nation", "market_segment")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create schema if it doesn't exist for the prepared dataset

# COMMAND ----------

display(spark.sql(f"CREATE SCHEMA IF NOT EXISTS main.{schema}"))
display(spark.sql(f"GRANT ALL PRIVILEGES ON SCHEMA main.{schema} TO `account users`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the prepared dataset in the appropriate schema

# COMMAND ----------

demo_dataset.write.mode("overwrite").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define the masking function and apply it in the table

# COMMAND ----------

display(spark.sql(f"ALTER TABLE {table_name} DROP ROW FILTER"))
display(
    spark.sql(
        f"""
CREATE
OR REPLACE FUNCTION main.{schema}.{user}_uc_mask_row_filter_func(nation_param STRING) RETURN (
  (
    current_user() == "{user_email}" AND nation_param == "INDIA"
  )
  or 
  (
    current_user() != "{user_email}"
  )
);
"""
    )
)
display(
    spark.sql(
        f"GRANT ALL PRIVILEGES ON FUNCTION main.{schema}.{user}_uc_mask_row_filter_func TO `account users`;"
    )
)
display(
    spark.sql(
        f"ALTER TABLE {table_name} SET ROW FILTER main.{schema}.{user}_uc_mask_row_filter_func ON (nation);"
    )
)
