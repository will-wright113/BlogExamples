# Databricks notebook source
import os

# COMMAND ----------

def register_delta(root,database=None):
  for root, dirs, files in os.walk(root):
    for name in dirs:
      if name == "_delta_log":
        location = root[5:]
        delta_name_base = root[10:]
        delta_name = delta_name_base.replace("/","_")
        if database == None:
          spark.sql("CREATE TABLE IF NOT EXISTS {delta_name} USING DELTA LOCATION '{location}' ".format(delta_name=delta_name,location=location))
        else:
          spark.sql("CREATE DATABASE IF NOT EXISTS {database}".format(database=database))
          spark.sql("CREATE TABLE IF NOT EXISTS {database}.{delta_name} USING DELTA LOCATION '{location}' ".format(database=database,delta_name=delta_name,location=location))
