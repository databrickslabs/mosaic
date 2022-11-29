# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup temporary data location
# MAGIC We will setup a temporary location to store our London Cycling data. </br>

# COMMAND ----------

user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

raw_path = f"dbfs:/tmp/mosaic/{user_name}"
raw_uprns_path = f"{raw_path}/cycling"

print(f"The raw data will be stored in {raw_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download UPRNs zip file

# COMMAND ----------

import requests
import pathlib

uprns_url = 'https://api.os.uk/downloads/v1/products/OpenUPRN/downloads?area=GB&format=CSV&redirect'

# The DBFS file system is mounted under /dbfs/ directory on Databricks cluster nodes

local_uprns_path = pathlib.Path(raw_uprns_path.replace('dbfs:/', '/dbfs/'))
local_uprns_path.mkdir(parents=True, exist_ok=True)

req = requests.get(uprns_url)
with open(local_uprns_path / f'uprns.zip', 'wb') as f:
  f.write(req.content)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the zip file was downloaded

# COMMAND ----------

display(dbutils.fs.ls(raw_uprns_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unzip the Zip file and create a delta table

# COMMAND ----------

import zipfile
zip_file_path = f"{raw_uprns_path}/uprns.zip".replace("dbfs:/", "/dbfs/")
data_path = f"{raw_uprns_path}/data/"
dbutils.fs.mkdirs(data_path)
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(data_path.replace("dbfs:/", "/dbfs/"))

# COMMAND ----------

display(dbutils.fs.ls(f"{raw_uprns_path}/data/"))

# COMMAND ----------

df = spark.read.option("header", "true").option("inferSchema" , "true").csv(f"{raw_uprns_path}/data/osopenuprn_202209.csv")
df.display()

# COMMAND ----------

spark.sql("drop table if exists uprns_table")

# COMMAND ----------

from pyspark.sql import functions as F
columns = [F.col(cn).alias(cn.replace(' ', '')) for cn in df.columns]
df.select(*columns).write.format("delta").mode("overwrite").saveAsTable("uprns_table")

# COMMAND ----------

df = spark.read.table("uprns_table")
df.count()

# COMMAND ----------


