// Databricks notebook source
// MAGIC %md
// MAGIC # Mosaic & Sedona
// MAGIC 
// MAGIC You can combine the usage of Mosaic with other geospatial libraries. 
// MAGIC 
// MAGIC In this example we combine the use of [Sedona](https://sedona.apache.org/setup/databricks/) and Mosaic.
// MAGIC 
// MAGIC ## Setup
// MAGIC 
// MAGIC This notebook will run if you have both Mosaic and Sedona installed on your cluster.
// MAGIC 
// MAGIC ### Install sedona
// MAGIC 
// MAGIC To install Sedona, follow the [official Sedona instructions](https://sedona.apache.org/setup/databricks/).

// COMMAND ----------

// Register Sedona in the 'default' database 
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
SedonaSQLRegistrator.registerAll(spark)

// Import Mosaic functions
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.H3
import com.databricks.labs.mosaic.ESRI

val mosaicContext = MosaicContext.build(H3, ESRI)
import mosaicContext.functions._
import org.apache.spark.sql.functions._

// COMMAND ----------

// Example dataset
val df = spark.createDataFrame(Seq(Tuple1("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))).toDF("wkt")

// COMMAND ----------

df
   .withColumn("mosaic_area", st_area($"wkt"))                      // Mosaic
   .withColumn("sedona_area", expr("ST_Area(ST_GeomFromWKT(wkt))")) // Sedona
   .withColumn("sedona_flipped", expr("ST_FlipCoordinates(ST_GeomFromWKT(wkt))")) // Sedona
   .show()

// COMMAND ----------


