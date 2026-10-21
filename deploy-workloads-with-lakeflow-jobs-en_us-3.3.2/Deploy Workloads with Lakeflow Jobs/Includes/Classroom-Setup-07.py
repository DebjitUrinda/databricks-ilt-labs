# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

display_config_values([('Your Catalog',my_catalog),('Your Schema',"jobs")])

# COMMAND ----------

def copy_data():
    base_location = '/Volumes/dbacademy_retail/v01/source_files/customers.csv'
    target_location = f'/Volumes/{my_catalog}/jobs/trigger_storage_location/'
    dbutils.fs.cp(base_location, target_location, True)
    print(f'Data has been copied from {base_location} to {target_location}')