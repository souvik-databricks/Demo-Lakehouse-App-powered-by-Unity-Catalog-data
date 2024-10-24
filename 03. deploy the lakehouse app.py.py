# Databricks notebook source
# MAGIC %md
# MAGIC # Deploying the demo lakehouse app
# MAGIC
# MAGIC This notebook uses the apps API to create and deploy the app. (If required also stop and delete the demo app.)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loading the configurations

# COMMAND ----------

# MAGIC %run ./configs.py

# COMMAND ----------

import requests
import json
import time

# COMMAND ----------

from databricks.sdk.core import Config
cfg = Config()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get host and token for the API calls

# COMMAND ----------

host = cfg.host
headers = cfg.authenticate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating the app identity - a service principal, oauth, and DNS entry
# MAGIC
# MAGIC Once the app is created, we can use the app identity for deployment and development iterations if required.

# COMMAND ----------

url = host + "/api/2.0/apps"
data = {
  "name": app_name,
  "description": app_description,
}
response = requests.post(url, headers=headers, json=data)
response.json()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking the app identity creation
# MAGIC
# MAGIC The create API is async, so we need to poll until it's ready.
# MAGIC

# COMMAND ----------

url = host + f"/api/2.0/apps/{app_name}"

for _ in range(10):
    time.sleep(2)
    response = requests.get(url, headers=headers)
    response_dict = json.loads(response.text)
    # print(f"Status: ", response_dict)
    if response_dict.get("compute_status").get("state") == "ACTIVE":
        break
response_dict

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now deploying the app using the created app identity
# MAGIC
# MAGIC Deploy starts the pod, downloads the source code, install necessary dependencies, and starts the app.
# MAGIC
# MAGIC Once the app is created, we can deploy the app. This will create a deployment and a compute target for the app. 
# MAGIC
# MAGIC The deployment will be in the "In Progress" state until the deployment is ready. 
# MAGIC
# MAGIC This can take a few minutes if we have to provision a new container. 
# MAGIC
# MAGIC After the container is provisioned, it should only take a few seconds to deploy the app again.

# COMMAND ----------

url = host + f"/api/2.0/apps/{app_name}/deployments"
data = {
  "source_code_path": app_dir,
  # "mode": "AUTO_SYNC" # SNAPSHOT is default and AUTO_SYNC is a private preview
}
response = requests.post(url, headers=headers, json=data)
response_dict = json.loads(response.text)
deployment_id = response_dict.get("deployment_id")
response_dict

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking the deployment status
# MAGIC
# MAGIC The first time the app is deployed, it can take a couple of minutes to deploy the app. 
# MAGIC
# MAGIC We will poll for the deployment to complete with the below code

# COMMAND ----------

url = host + f"/api/2.0/apps/{app_name}/deployments/{deployment_id}"

for _ in range(60):
    time.sleep(5)
    response = requests.get(url, headers=headers)
    response_dict = json.loads(response.text)
    # response_dict
    if response_dict.get("status").get("state") == "SUCCEEDED":
        print(f"Deployment of the app '{app_name}' completed. Status: ", response_dict.get("status").get("state"))
        break
    else:
        print(f"Deployment of the app '{app_name}' in progress. Status: ", response_dict.get("status").get("state"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Get the deployed demo app's information

# COMMAND ----------

url = host + f"/api/2.0/apps/{app_name}"

response = requests.get(url, headers=headers)
response.json()
response_dict = json.loads(response.text)
print(f"Deployment of the app '{app_name}' is ready at", response_dict.get("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stop the app (if required uncomment and use)

# COMMAND ----------

# url = host + f"/api/2.0/preview/apps/{app_name}/stop"

# response = requests.post(url, headers=headers)
# response.json()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete the app (if required uncomment and use)

# COMMAND ----------

# url = host + f"/api/2.0/preview/apps/{app_name}"

# response = requests.delete(url, headers=headers)
# response.json()
