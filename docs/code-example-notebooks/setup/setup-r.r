# Databricks notebook source
library(SparkR)

# COMMAND ----------

user <- collect(sql("SELECT current_user() as user"))$user

# COMMAND ----------

install.packages(paste0("/dbfs/FileStore/shared_uploads/", user, "/sparkrMosaic_0_1_0_tar.gz"), repos=NULL)

# COMMAND ----------

library(sparkrMosaic)
enableMosaic()
