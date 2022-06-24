# Databricks notebook source
from pyspark.sql.functions import *
from mosaic import *
enable_mosaic(spark, dbutils)

# COMMAND ----------

user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
