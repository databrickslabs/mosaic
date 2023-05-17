# Databricks notebook source
# MAGIC %sql
# MAGIC USE flood_risk

# COMMAND ----------

df = spark.read.table("to_score")

# COMMAND ----------

# MAGIC %md
# MAGIC Load the pretrained model from AutoML.

# COMMAND ----------

import mlflow
import pyspark.sql.functions as F
from pyspark.sql.functions import struct, col
logged_model = 'runs:/a51b3d3b1caa41af894f50efc988d60b/model'

# Load model as a Spark UDF. Override result_type if the model does not return double values.
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model, result_type='double')

# COMMAND ----------

# Predict on a Spark DataFrame.
results = df.withColumn('predictions', loaded_model(struct(*map(col, df.columns))))
results.display()

# COMMAND ----------

results.write.mode("overwrite").saveAsTable("scored")

# COMMAND ----------

scored = spark.read.table("scored")

# COMMAND ----------


