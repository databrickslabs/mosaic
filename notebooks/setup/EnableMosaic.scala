// Databricks notebook source
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.OGC
import com.databricks.labs.mosaic.H3
val mosaicContext: MosaicContext = MosaicContext(H3, OGC)
import mosaicContext.functions._

// COMMAND ----------

// MAGIC %run "./GeneratorsHotfix"

// COMMAND ----------

import com.databricks.labs.mosaic.patch.MosaicPatch
import com.databricks.labs.mosaic.OGC
import com.databricks.labs.mosaic.H3
val mosaicPatch = MosaicPatch(H3, OGC)
import mosaicPatch.functions._

// COMMAND ----------

mosaicContext.register(spark)
mosaicPatch.register(spark)

// COMMAND ----------

// MAGIC %run "./PythonBindings"
