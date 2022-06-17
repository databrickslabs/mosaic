// Databricks notebook source
// MAGIC %md
// MAGIC # Code examples for Mosaic documentation

// COMMAND ----------

// MAGIC %md
// MAGIC ## Setup

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import *
// MAGIC from mosaic import *
// MAGIC enable_mosaic(spark, dbutils)

// COMMAND ----------

import org.apache.spark.sql.functions._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.ESRI
import com.databricks.labs.mosaic.H3

val mosaicContext: MosaicContext = MosaicContext.build(H3, ESRI)

// COMMAND ----------

import mosaicContext.functions._
import spark.implicits._
mosaicContext.register(spark)

// COMMAND ----------

// MAGIC %r
// MAGIC install.packages("/dbfs/FileStore/shared_uploads/stuart.lynn@databricks.com/sparkrMosaic_0_1_0_tar.gz", repos=NULL)

// COMMAND ----------

// MAGIC %r
// MAGIC library(SparkR)
// MAGIC library(sparkrMosaic)
// MAGIC enableMosaic()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Geometry constructors

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_point

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'lon': 30., 'lat': 10.}])
// MAGIC df.select(st_point('lon', 'lat').alias('point_geom')).show(1, False)

// COMMAND ----------

val df = List((30.0, 10.0)).toDF("lon", "lat")
df.select(st_point($"lon", $"lat")).alias("point_geom").show()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_point(30D, 10D) AS point_geom

// COMMAND ----------

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(lon = 30.0, lat = 10.0))
// MAGIC showDF(select(df, alias(st_point(column("lon"), column("lat")), "point_geom")), truncate=F)

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_makeline

// COMMAND ----------

// MAGIC %python
// MAGIC  df = spark.createDataFrame([
// MAGIC   {'lon': 30., 'lat': 10.},
// MAGIC   {'lon': 10., 'lat': 30.},
// MAGIC   {'lon': 40., 'lat': 40.}
// MAGIC ])
// MAGIC (
// MAGIC   df.select(st_point('lon', 'lat').alias('point_geom'))
// MAGIC   .groupBy()
// MAGIC   .agg(collect_list('point_geom').alias('point_array'))
// MAGIC   .select(st_makeline('point_array').alias('line_geom'))
// MAGIC ).show(1, False)

// COMMAND ----------

val df = List(
  (30.0, 10.0),
  (10.0, 30.0),
  (40.0, 40.0)
  ).toDF("lon", "lat")
df.select(st_point($"lon", $"lat").alias("point_geom"))
  .groupBy()
  .agg(collect_list($"point_geom").alias("point_array"))
  .select(st_makeline($"point_array").alias("line_geom"))
  .show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC SET spark.sql.shuffle.partitions=1;
// MAGIC WITH points (
// MAGIC   SELECT st_point(30D, 10D) AS point_geom
// MAGIC   UNION SELECT st_point(10D, 30D) AS point_geom
// MAGIC   UNION SELECT st_point(40D, 40D) AS point_geom)
// MAGIC SELECT st_makeline(collect_list(point_geom))
// MAGIC FROM points

// COMMAND ----------

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(lon = c(30.0, 10.0, 40.0), lat = c(10.0, 30.0, 40.0)))
// MAGIC df <- select(df, alias(st_point(column("lon"), column("lat")), "point_geom"))
// MAGIC df <- groupBy(df)
// MAGIC df <- agg(df, alias(collect_list(column("point_geom")), "point_array"))
// MAGIC df <- select(df, alias(st_makeline(column("point_array")), "line_geom"))
// MAGIC showDF(df, truncate=F)

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_makepolygon

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'LINESTRING (30 10, 40 40, 20 40, 10 20, 30 10)'}])
// MAGIC df.select(st_makepolygon(st_geomfromwkt('wkt')).alias('polygon_geom')).show(1, False)

// COMMAND ----------

val df = List(("LINESTRING (30 10, 40 40, 20 40, 10 20, 30 10)")).toDF("wkt")
df.select(st_makepolygon(st_geomfromwkt($"wkt")).alias("polygon_geom")).show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_makepolygon(st_geomfromwkt("LINESTRING (30 10, 40 40, 20 40, 10 20, 30 10)")) AS polygon_geom

// COMMAND ----------

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame('wkt' = 'LINESTRING (30 10, 40 40, 20 40, 10 20, 30 10)'))
// MAGIC showDF(select(df, alias(st_makepolygon(st_geomfromwkt(column('wkt'))), 'polygon_geom')), truncate=F)

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_geomfromwkt

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'LINESTRING (30 10, 40 40, 20 40, 10 20, 30 10)'}])
// MAGIC df.select(st_geomfromwkt('wkt')).show(1, False)

// COMMAND ----------

val df = List(("LINESTRING (30 10, 40 40, 20 40, 10 20, 30 10)")).toDF("wkt")
df.select(st_geomfromwkt($"wkt")).show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_geomfromwkt("LINESTRING (30 10, 40 40, 20 40, 10 20, 30 10)") AS linestring

// COMMAND ----------

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame('wkt' = 'LINESTRING (30 10, 40 40, 20 40, 10 20, 30 10)'))
// MAGIC showDF(select(df, alias(st_geomfromwkt(column('wkt')), 'linestring')), truncate=F)

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_geomfromwkb

// COMMAND ----------

// MAGIC %python
// MAGIC import binascii
// MAGIC hex = '0000000001C052F1F0ED3D859D4041983D46B26BF8'
// MAGIC binary = binascii.unhexlify(hex)
// MAGIC df = spark.createDataFrame([{'wkb': binary}])
// MAGIC df.select(st_geomfromwkb('wkb')).show(1, False)

// COMMAND ----------

val df = List(("POINT (-75.78033 35.18937)")).toDF("wkt")
df.select(st_geomfromwkb(st_aswkb($"wkt"))).show(false)

// COMMAND ----------

import org.locationtech.jts.io.{WKTReader, WKBWriter}
val reader = new WKTReader()
val writer = new WKBWriter()
val geom = reader.read("POINT (-75.78033 35.18937)")
val bytes = writer.write(geom)
// BigInt(hex, 16).toByteArray

// COMMAND ----------

val hex = "0000000001C052F1F0ED3D859D4041983D46B26BF8"
val paddedBytes = new Array[Byte](hex.length / 2)
val bytes = BigInt(hex, 16).toByteArray
Array.copy(bytes, 0, paddedBytes, paddedBytes.length - bytes.length, bytes.length)

val df = List((paddedBytes)).toDF("wkb")
df.select(st_geomfromwkb($"wkb")).show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_geomfromwkb(st_aswkb("POINT (-75.78033 35.18937)"))

// COMMAND ----------

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame('wkt'= "POINT (-75.78033 35.18937)"))
// MAGIC showDF(select(df, st_geomfromwkb(st_aswkb(column("wkt")))), truncate=F)

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_geomfromgeojson

// COMMAND ----------

// MAGIC %python
// MAGIC import json
// MAGIC geojson_dict = {
// MAGIC     "type":"Point",
// MAGIC     "coordinates":[
// MAGIC         -75.78033,
// MAGIC         35.18937
// MAGIC     ],
// MAGIC     "crs":{
// MAGIC         "type":"name",
// MAGIC         "properties":{
// MAGIC             "name":"EPSG:0"
// MAGIC         }
// MAGIC     }
// MAGIC }
// MAGIC df = spark.createDataFrame([{'json': json.dumps(geojson_dict)}])
// MAGIC df.select(st_geomfromgeojson('json')).show(1, False)

// COMMAND ----------

val df = List(
  ("""{
      |   "type":"Point",
      |   "coordinates":[
      |       -75.78033,
      |       35.18937
      |   ],
      |   "crs":{
      |       "type":"name",
      |       "properties":{
      |           "name":"EPSG:0"
      |       }
      |   }
      |}""".stripMargin)
    )
    .toDF("json")
df.select(st_geomfromgeojson($"json")).show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_geomfromgeojson("{\"type\":\"Point\",\"coordinates\":[-75.78033,35.18937],\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:0\"}}}")

// COMMAND ----------

// MAGIC %r
// MAGIC geojson <- '{
// MAGIC       "type":"Point",
// MAGIC       "coordinates":[
// MAGIC           -75.78033,
// MAGIC           35.18937
// MAGIC       ],
// MAGIC       "crs":{
// MAGIC           "type":"name",
// MAGIC           "properties":{
// MAGIC               "name":"EPSG:0"
// MAGIC           }
// MAGIC       }
// MAGIC   }'
// MAGIC df <- createDataFrame(data.frame('json' = geojson))
// MAGIC showDF(select(df, st_geomfromgeojson(column('json'))), truncate=F)
