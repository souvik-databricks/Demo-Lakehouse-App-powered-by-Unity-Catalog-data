# Databricks notebook source
# MAGIC %md
# MAGIC # Common configurations for the demo setup
# MAGIC
# MAGIC 1. Configure the `schema` and `table` that will be used for the demo.
# MAGIC 2. Configure the `app directory` in which the app will be created.
# MAGIC 3. Configure the `Warehouse ID` which will be used by the App to pull data.
# MAGIC 4. Configure the `app name` and `app description`. (Remember app names can only be alphanumeric and contain hyphens).
# MAGIC

# COMMAND ----------

schema = "lha_with_uc"
user_email = spark.sql("select current_user()").collect()[0]["current_user()"]
user = user_email.split("@")[0].replace(".", "_")
table_name = f"main.{schema}.{user}_lha_with_uc"
print("user_email: ", user_email, "\n", "schema: ", schema, "\n", "table_name: ", table_name, sep="")

# COMMAND ----------

current_dir = !pwd
current_dir = current_dir[0]
app_dir = f"{current_dir}/lha_with_uc_app"
print("current_dir: ", current_dir, "\n", "app_dir: ", app_dir, sep="")

# COMMAND ----------

# Edit your warehouse_id here (if required)
warehouse_id = "4fe75792cd0d304c"
print("warehouse_id: ", warehouse_id, sep="")

# COMMAND ----------

# Edit the app_name here (if required)
app_name = f"{user_email.split('@')[0].split('.')[0]}-lha-with-uc-app"
app_description = "Field Demo: Lakehouse App powered by unity catalog masked data."
print("app_name: ", app_name, "\n", "app_description: ", app_description, sep="")
