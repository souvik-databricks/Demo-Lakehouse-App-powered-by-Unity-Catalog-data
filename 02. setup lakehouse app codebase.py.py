# Databricks notebook source
# MAGIC %md
# MAGIC # Creating the Demo Lakehouse App
# MAGIC
# MAGIC In the previous notebook we prepared the data part of our demo. Now here we will be creating the actual web app with which the data, that we prepared and applied mask previously, will integrate with.
# MAGIC
# MAGIC The App is a simple streamlit app which is going to allow for the following behaviour:
# MAGIC > 1. The app captures user info and show some of the basic info in the UI for the user to see. 
# MAGIC > 
# MAGIC > 2. It will allow for `choosing the particular mode of authentication (User / App's Service Principal) for interacting with the UC data.`
# MAGIC >
# MAGIC > 3. `Based on the type of authentication` the app will pull specific data only due to the `masking rule of the UC data`.
# MAGIC >
# MAGIC > 4. The app also will show `interactive visualization` of the data in the web app. ( Thanks to plotly.express 😋 )
# MAGIC >
# MAGIC > 5. Their's some more advanced tabs in the app which will show various kinds of information related to the headers & cookies and the environment of the serverless compute in which the app is running.<br></br> (&nbsp;  This information might not be useful for this demo directly but can be used for advanced use cases like determining the country of origin from where the user is querying. and then automatically filtering the data for that country. <br></br>  _Here also, the app uses the IP address information to determine the user's location._ 😉  &nbsp; ) 

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ### Loading the configurations

# COMMAND ----------

# MAGIC %run ./configs.py

# COMMAND ----------

# MAGIC %md
# MAGIC ### Instantiating the workspace client for creating the app directory and files 

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating the app directory

# COMMAND ----------

w.workspace.mkdirs(app_dir)

# COMMAND ----------

from databricks.sdk.service.workspace import ImportFormat
import io

# COMMAND ----------

# MAGIC %md
# MAGIC ### `requirements.txt` created

# COMMAND ----------

requirements = """streamlit==1.38.0
pydeck
protobuf
plotly==5.24.0
"""
requirements_bytes = requirements.encode('utf-8')
file = f'{app_dir}/requirements.txt'
w.workspace.upload(file, io.BytesIO(requirements_bytes), format=ImportFormat.AUTO)

# COMMAND ----------

# MAGIC %md
# MAGIC ### `app.yaml` created

# COMMAND ----------

yaml = f"""
command:
  - "streamlit"
  - "run"
  - "app.py"

env:
- name: "DATABRICKS_WAREHOUSE_ID"
  value: "{warehouse_id}"
- name: STREAMLIT_BROWSER_GATHER_USAGE_STATS
  value: "false"
"""
yaml_bytes = yaml.encode('utf-8')
file = f'{app_dir}/app.yaml'
w.workspace.upload(file, io.BytesIO(yaml_bytes), format=ImportFormat.AUTO)

# COMMAND ----------

# MAGIC %md
# MAGIC ### `app.py` created

# COMMAND ----------

app_code = f"""
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import numpy as np
import pandas as pd
import plotly.express as px
import requests
import os

cfg = Config()

def _get_location_flag():
    ip = st.context.headers['X-Forwarded-For'].split(',')[0]
    get_ip_location_info = requests.get(f"http://ip-api.com/json/{{ip}}?fields=countryCode,regionName,city")
    country_code = get_ip_location_info.json()['countryCode'].lower()
    location_flag_code = f":flag-{{country_code}}:"
    region_name = get_ip_location_info.json()['regionName']
    city = get_ip_location_info.json()['city']
    return f"{{city}}, {{region_name}}, {{location_flag_code}}"

def _get_user_info():
    headers = st.context.headers
    return dict(
        user_name=headers.get("X-Forwarded-Preferred-Username"),
        user_email=headers.get("X-Forwarded-Email"),
        user_id=headers.get("X-Forwarded-User"),
        access_token=headers.get("X-Forwarded-Access-Token")
    )
user_info = _get_user_info()

def _get_user_credentials() -> dict[str, str]:
    return dict(Authorization=f"Bearer {{user_info.get('access_token')}}")

st.set_page_config(layout="wide", page_title="LHApp + UC Demo", page_icon=":rosette:")

# add current user info
st.sidebar.title("User Info & Authentication Method")

st.sidebar.subheader("",divider="violet")
st.sidebar.write(f"User Email: {{user_info.get('user_email')}}")
st.sidebar.write(f"User ID: {{user_info.get('user_id').split('@')[0]}}")
st.sidebar.write(f"User Location: {{_get_location_flag()}}")
st.sidebar.subheader("",divider="violet")

service_principal = 'Service Principal'
app_user = 'App User'
options = [app_user, service_principal]
default_index = options.index(app_user)
st.session_state.authentication_method = st.sidebar.selectbox('Choose the authentication method', options, index=default_index)


def get_credentials(method):
    if method == app_user:
        return lambda: _get_user_credentials
    else:
        return lambda: cfg.authenticate


@st.cache_data(ttl=30)
def get_data(method):
    os.write(1, f"Fetching data from Databricks using {{method}}\\n".encode())
    os.write(1, f"{{get_credentials(method)()()}}\\n".encode())

    connection = sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{{os.getenv('DATABRICKS_WAREHOUSE_ID')}}",
        credentials_provider=get_credentials(method),
    )
    cursor = connection.cursor()

    try:
        cursor.execute(
            "SELECT * FROM {table_name};"
        )
        df = cursor.fetchall_arrow().to_pandas()
        return df
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        connection.close()


def get_uc_df():
    return get_data(st.session_state.authentication_method)

st.header("Welcome to CME Vertical of Databricks")
data = get_uc_df()
count_data = data.groupby(['nation','market_segment']).size().reset_index(name='count')


tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["Full Data :bookmark_tabs:",
                      "Visualized Data :bar_chart:",
                      "Headers :dove_of_peace:",
                      "Cookies :cookie:",
                      "Environment :computer:",
                      "Service Principal :robot_face:"])

with tab1:
    st.subheader("Full Data", divider="violet")
    st.dataframe(data=data, height=400, use_container_width=True)

with tab2:
    st.subheader("Visualized Data", divider="violet")
    fig = px.bar(count_data, x='nation', y='count', color='market_segment', barmode='group')
    st.plotly_chart(fig)

with tab3:
    st.subheader("Captured Headers", divider="violet")
    st.json(st.context.headers)

with tab4:
    st.subheader("Captured Cookies", divider="violet")
    st.json(st.context.cookies)

with tab5:
    st.subheader("Environment Variables", divider="violet")
    st.json(dict(os.environ))

with tab6:
    st.subheader("App Service Principal Details", divider="violet")
    auth_obj = cfg.authenticate()['Authorization']
    sp_self_api_call = requests.get(f"https://{{os.environ['DATABRICKS_HOST']}}/api/2.0/preview/scim/v2/Me", headers={{"Authorization": auth_obj}})
    st.json(sp_self_api_call.json())

"""
app_code_bytes = app_code.encode('utf-8')
file = f'{app_dir}/app.py'
w.workspace.upload(file, io.BytesIO(app_code_bytes), format=ImportFormat.AUTO)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating `.streamlit` and inside it `config.toml` for theming the app

# COMMAND ----------

w.workspace.mkdirs(f"{app_dir}/.streamlit")

theme_setting = """[theme]
primaryColor="#6eb52f"
backgroundColor="#f0f0f5"
secondaryBackgroundColor="#e0e0ef"
textColor="#262730"
font="sans serif"
"""
theme_setting_bytes = theme_setting.encode('utf-8')
file = f'{app_dir}/.streamlit/config.toml'
w.workspace.upload(file, io.BytesIO(theme_setting_bytes), format=ImportFormat.AUTO)

# COMMAND ----------

# MAGIC %md
# MAGIC ### In case of cleanup after demo uncomment and run the below line for deleting the app directory along with the files and folder(s) inside it

# COMMAND ----------

# w.workspace.delete(f"{app_dir}",True)
