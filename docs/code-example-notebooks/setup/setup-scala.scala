// Databricks notebook source
import org.apache.spark.sql.functions._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.JTS
import com.databricks.labs.mosaic.H3

val mosaicContext: MosaicContext = MosaicContext.build(H3, JTS)

// COMMAND ----------

import mosaicContext.functions._
import spark.implicits._
mosaicContext.register(spark)

// COMMAND ----------

val user = dbutils.notebook.getContext.tags("user")
