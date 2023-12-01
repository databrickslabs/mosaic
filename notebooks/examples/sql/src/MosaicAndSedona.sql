-- Databricks notebook source
-- MAGIC %md # Mosaic & Sedona
-- MAGIC
-- MAGIC > You can combine the usage of [Mosaic](https://databrickslabs.github.io/mosaic/index.html) with other geospatial libraries. In this example we combine it with [Sedona](https://sedona.apache.org).
-- MAGIC
-- MAGIC ## Setup
-- MAGIC
-- MAGIC This notebook will run if you have both Mosaic and Sedona installed on your cluster as described below.
-- MAGIC
-- MAGIC ### Install Sedona
-- MAGIC
-- MAGIC To install Sedona, follow the [official Sedona instructions](https://sedona.apache.org/1.5.0/setup/databricks/).
-- MAGIC
-- MAGIC E.g. Add the following maven coordinates to a non-photon cluster [[1](https://docs.databricks.com/en/libraries/package-repositories.html)]. This is showing DBR 12.2 LTS.  
-- MAGIC
-- MAGIC ```
-- MAGIC org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.5.0
-- MAGIC org.datasyslab:geotools-wrapper:1.5.0-28.2
-- MAGIC ```
-- MAGIC
-- MAGIC ### Install Mosaic
-- MAGIC
-- MAGIC Download Mosaic JAR to your local machine (e.g. from [here](https://github.com/databrickslabs/mosaic/releases/download/v_0.3.12/mosaic-0.3.12-jar-with-dependencies.jar) for 0.3.12) and then UPLOAD to your cluster [[1](https://docs.databricks.com/en/libraries/cluster-libraries.html#install-a-library-on-a-cluster)]. 
-- MAGIC
-- MAGIC ### Notes
-- MAGIC
-- MAGIC * This is for [SPARK SQL](https://www.databricks.com/glossary/what-is-spark-sql#:~:text=Spark%20SQL%20is%20a%20Spark,on%20existing%20deployments%20and%20data.) which is different from [DBSQL](https://www.databricks.com/product/databricks-sql); __The best way to combine is to not register mosaic SQL functions since Sedona is primarily SQL.__
-- MAGIC * See instructions for `SedonaContext.create(spark)` [[1](https://sedona.apache.org/1.5.0/tutorial/sql/?h=sedonacontext#initiate-sedonacontext)]. 
-- MAGIC * And, Sedona identifies that it might have issues if executed on a [Photon](https://www.databricks.com/product/photon) cluster; again this example is showing DBR 12.2 LTS on the Mosaic 0.3 series.
-- MAGIC
-- MAGIC --- 
-- MAGIC  __Last Update__ 01 DEC 2023 [Mosaic 0.3.12]

-- COMMAND ----------

-- MAGIC %md ## Prior to Setup
-- MAGIC
-- MAGIC > Notice that even in DBR 12.2 LTS, Databricks initially has gated functions, meaning they will not execute on the runtime but are there. However, we will see that after registering functions, e.g. from Sedona, those then become available (in DBR).

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC -- before we do anything
-- MAGIC -- have gated product functions
-- MAGIC show system functions like 'st_*'

-- COMMAND ----------

-- MAGIC %md _The following exception will be thrown if you attempt to execute the gated functions:_
-- MAGIC
-- MAGIC ```
-- MAGIC AnalysisException: [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve "st_area(POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10)))" due to data type mismatch: parameter 1 requires ("GEOMETRY" or "GEOGRAPHY") type, however, "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))" is of "STRING" type.; line 1 pos 7;
-- MAGIC 'Project [unresolvedalias(st_area(POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))), None)]
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC -- assumes you are in DBR 12.2 LTS
-- MAGIC -- so this will not execute
-- MAGIC -- uncomment to verify
-- MAGIC -- select st_area('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))')

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC -- notice, e.g. these are initially gated product functions
-- MAGIC describe function extended st_area

-- COMMAND ----------

-- MAGIC %md ## Setup
-- MAGIC
-- MAGIC > We are installing Mosaic without SQL functions registered (via Scala) and are installing Sedona SQL as normal.

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC
-- MAGIC // -- spark functions
-- MAGIC import org.apache.spark.sql.functions._
-- MAGIC
-- MAGIC // -- mosaic functions
-- MAGIC import com.databricks.labs.mosaic.functions.MosaicContext
-- MAGIC import com.databricks.labs.mosaic.H3
-- MAGIC import com.databricks.labs.mosaic.JTS
-- MAGIC
-- MAGIC val mosaicContext = MosaicContext.build(H3, JTS)
-- MAGIC import mosaicContext.functions._
-- MAGIC
-- MAGIC // ! don't register SQL functions !
-- MAGIC // - this allows sedona to be the main spatial SQL provider
-- MAGIC //mosaicContext.register()
-- MAGIC
-- MAGIC // -- sedona functions
-- MAGIC import org.apache.sedona.spark.SedonaContext
-- MAGIC val sedona = SedonaContext.create(spark)

-- COMMAND ----------

-- MAGIC %md _Now when we list user functions, we see all the Sedona provided ones._

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC show user functions like 'st_*'

-- COMMAND ----------

-- MAGIC %md _Notice that the prior system registered functions have been replaced, e.g. `ST_Area`._

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC -- notice, e.g. the provided function now are available
-- MAGIC describe function extended st_area

-- COMMAND ----------

-- MAGIC %md ## Queries
-- MAGIC
-- MAGIC > Showing how Sedona (registered Spark SQL) and Mosaic (Scala) can co-exist on the same cluster. Not shown here, but the could also be Mosaic Python bindings.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW sample AS (
  SELECT 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))' AS wkt
);

SELECT * FROM sample

-- COMMAND ----------

-- MAGIC %md _Here is a Spark SQL call to use the Sedona functions._

-- COMMAND ----------

SELECT ST_Area(ST_GeomFromText(wkt)) AS sedona_area FROM sample

-- COMMAND ----------

-- MAGIC %md _Here is Scala call to the same Mosaic-provided `ST_Area` function._

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // verify scala functions registered
-- MAGIC display(
-- MAGIC   spark
-- MAGIC   .table("sample")
-- MAGIC     .select(st_area($"wkt").as("mosaic_area"))
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md _Mosaic + Sedona_
-- MAGIC
-- MAGIC > Showing blending Mosaic calls (in Scala) with Sedona (Spark SQL) calls.

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC display(
-- MAGIC   spark.table("sample")
-- MAGIC     .select(
-- MAGIC       st_area($"wkt").as("mosaic_area"),                    // <- mosaic (scala)
-- MAGIC       expr("ST_Area(ST_GeomFromText(wkt)) AS sedona_area"), // <- sedona (spark sql)
-- MAGIC       $"wkt"
-- MAGIC     )
-- MAGIC )
