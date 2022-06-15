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

// MAGIC %md
// MAGIC ## Geometry accessors

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_asbinary / st_aswkb

// COMMAND ----------

val df = List(("POINT (30 10)")).toDF("wkt")
df.select(st_asbinary($"wkt").alias("wkb")).show()

// COMMAND ----------

// MAGIC %sql
// MAGIC select st_asbinary("POINT (30 10)") as wkb

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'POINT (30 10)'}])
// MAGIC df.select(st_asbinary('wkt').alias('wkb')).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_asgeojson

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'POINT (30 10)'}])
// MAGIC df.select(st_asgeojson('wkt').cast('string').alias('json')).show(truncate=False)

// COMMAND ----------

// MAGIC %scala
// MAGIC val df = List(("POINT (30 10)")).toDF("wkt")
// MAGIC df.select(st_asgeojson($"wkt").cast("string").alias("json")).show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT cast(st_asgeojson("POINT (30 10)") as string) AS json

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_astext / st_aswkt

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'lon': 30., 'lat': 10.}])
// MAGIC df.select(st_astext(st_point('lon', 'lat')).alias('wkt')).show()

// COMMAND ----------

val df = List((30.0, 10.0)).toDF("lon", "lat")
df.select(st_astext(st_point($"lon", $"lat")).alias("wkt")).show()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT ST_AsText(ST_Point(30D, 10D)) AS wkt

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

// MAGIC %md
// MAGIC ## Spatial functions

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_area

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
// MAGIC df.select(st_area('wkt')).show()

// COMMAND ----------

val df = List(("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("wkt")
df.select(st_area($"wkt")).show()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_area("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_buffer

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
// MAGIC df.select(st_buffer('wkt', lit(2.))).show()

// COMMAND ----------

val df = List(("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("wkt")
df.select(st_buffer($"wkt", 2d)).show()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_buffer("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))", 2d)

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_length

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
// MAGIC df.select(st_length('wkt')).show()

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
// MAGIC df.select(st_perimeter('wkt')).show()

// COMMAND ----------

val df = List(("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("wkt")
df.select(st_length($"wkt")).show()

// COMMAND ----------

val df = List(("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("wkt")
df.select(st_perimeter($"wkt")).show()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_length("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_perimeter("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")

// COMMAND ----------

// MAGIC %md 
// MAGIC ### st_convexhull

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'MULTIPOINT ((10 40), (40 30), (20 20), (30 10))'}])
// MAGIC df.select(st_convexhull('wkt')).show(1, False)

// COMMAND ----------

val df = List(("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))")).toDF("wkt")
df.select(st_convexhull($"wkt")).show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_convexhull("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))")

// COMMAND ----------

// MAGIC %md 
// MAGIC ### st_dump

// COMMAND ----------

// MAGIC %python
// MAGIC help(st_dump)

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'MULTIPOINT ((10 40), (40 30), (20 20), (30 10))'}])
// MAGIC df.select(st_dump('wkt')).show(5, False)

// COMMAND ----------

val df = List(("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))")).toDF("wkt")
df.select(st_dump($"wkt")).show(false)

// COMMAND ----------

spark.sql("""SELECT st_dump("MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))")""").show()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_dump("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))")

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_srid

// COMMAND ----------

// MAGIC %python
// MAGIC help(st_srid)

// COMMAND ----------

// MAGIC %python
// MAGIC json_geom = '{"type":"MultiPoint","coordinates":[[10,40],[40,30],[20,20],[30,10]],"crs":{"type":"name","properties":{"name":"EPSG:4326"}}}'
// MAGIC df = spark.createDataFrame([{'json': json_geom}])
// MAGIC df.select(st_srid(as_json('json'))).show(1)

// COMMAND ----------

val df = 
  List("""{"type":"MultiPoint","coordinates":[[10,40],[40,30],[20,20],[30,10]],"crs":{"type":"name","properties":{"name":"EPSG:4326"}}}""")
  .toDF("json")
df.select(st_srid(as_json($"json"))).show(1)

// COMMAND ----------

// MAGIC %sql
// MAGIC set json.string='{"type":"MultiPoint","coordinates":[[10,40],[40,30],[20,20],[30,10]],"crs":{"type":"name","properties":{"name":"EPSG:4326"}}}';
// MAGIC select st_srid(as_json(${json.string}))

// COMMAND ----------

// MAGIC %sql
// MAGIC select st_srid(as_json('{"type":"MultiPoint","coordinates":[[10,40],[40,30],[20,20],[30,10]],"crs":{"type":"name","properties":{"name":"EPSG:4326"}}}'))

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_setsrid

// COMMAND ----------

// MAGIC %python
// MAGIC help(st_setsrid)

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'MULTIPOINT ((10 40), (40 30), (20 20), (30 10))'}])
// MAGIC df.select(st_setsrid(st_geomfromwkt('wkt'), lit(4326))).show(1)

// COMMAND ----------

val df = List("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))").toDF("wkt")
df.select(st_setsrid(st_geomfromwkt($"wkt"), lit(4326))).show

// COMMAND ----------

// MAGIC %sql
// MAGIC select st_setsrid(st_geomfromwkt("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"), 4326)

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_transform

// COMMAND ----------

// MAGIC %python
// MAGIC help(st_transform)

// COMMAND ----------

// MAGIC %python
// MAGIC df = (
// MAGIC   spark.createDataFrame([{'wkt': 'MULTIPOINT ((10 40), (40 30), (20 20), (30 10))'}])
// MAGIC   .withColumn('geom', st_setsrid(st_geomfromwkt('wkt'), lit(4326)))
// MAGIC )
// MAGIC df.select(st_astext(st_transform('geom', lit(3857)))).show(1, False)

// COMMAND ----------

val df = List("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))").toDF("wkt")
  .withColumn("geom", st_setsrid(st_geomfromwkt($"wkt"), lit(4326)))
df.select(st_astext(st_transform($"geom", lit(3857)))).show(1, false)

// COMMAND ----------

// MAGIC %sql
// MAGIC select st_astext(st_transform(st_setsrid(st_geomfromwkt("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"), 4326), 3857))

// COMMAND ----------

// MAGIC %md 
// MAGIC ### st_translate

// COMMAND ----------

// MAGIC %python
// MAGIC help(st_translate)

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'MULTIPOINT ((10 40), (40 30), (20 20), (30 10))'}])
// MAGIC df.select(st_translate('wkt', lit(10), lit(-5))).show(1, False)

// COMMAND ----------

val df = List(("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))")).toDF("wkt")
df.select(st_translate($"wkt", lit(10d), lit(-5d))).show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_translate("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))", 10d, -5d)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### st_scale

// COMMAND ----------

// MAGIC %python
// MAGIC help(st_scale)

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
// MAGIC df.select(st_scale('wkt', lit(0.5), lit(2))).show(1, False)

// COMMAND ----------

val df = List(("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("wkt")
df.select(st_scale($"wkt", lit(0.5), lit(2.0))).show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_scale("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))", 0.5d, 2.0d)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### st_rotate

// COMMAND ----------

// MAGIC %python
// MAGIC help(st_rotate)

// COMMAND ----------

// MAGIC %python
// MAGIC from math import pi
// MAGIC df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
// MAGIC df.select(st_rotate('wkt', lit(pi))).show(1, False)

// COMMAND ----------

import math.Pi
val df = List(("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("wkt")
df.select(st_rotate($"wkt", lit(Pi))).show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_rotate("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))", pi())

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_centroid2D

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
// MAGIC df2 = df.select(st_centroid2D('wkt'))
// MAGIC df2.show(1, False)

// COMMAND ----------

// MAGIC %python
// MAGIC df2.schema

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_isvalid

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
// MAGIC df.select(st_isvalid('wkt')).show()

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))'}])
// MAGIC df.select(st_isvalid('wkt')).show()

// COMMAND ----------

val df = List(("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("wkt")
df.select(st_isvalid($"wkt")).show()

// COMMAND ----------

val df = List(("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))")).toDF("wkt")
df.select(st_isvalid($"wkt")).show()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_isvalid("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_isvalid("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))")

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_geometrytype

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
// MAGIC df.select(st_geometrytype('wkt')).show()

// COMMAND ----------

val df = List(("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("wkt")
df.select(st_geometrytype($"wkt")).show()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_geometrytype("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))")

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_xmin

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
// MAGIC df.select(st_xmin('wkt')).show()

// COMMAND ----------

val df = List(("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("wkt")
df.select(st_xmin($"wkt")).show()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_xmin("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_xmax

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_ymin

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_ymax

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_zmin

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_zmax

// COMMAND ----------

// MAGIC %md 
// MAGIC ### st_distance

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'point': 'POINT (5 5)', 'poly': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
// MAGIC df.select(st_distance('poly', 'point')).show()

// COMMAND ----------

val df = List(("POINT (5 5)", "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("point", "poly")
df.select(st_distance($"poly", $"point")).show()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_distance("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))", "POINT (5 5)")

// COMMAND ----------

### 

// COMMAND ----------

// MAGIC %md
// MAGIC ### flatten_polygons

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))'}])
// MAGIC df.select(flatten_polygons('wkt')).show(2, False)

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'MULTIPOINT (30 20, 45 40, 10 40, 30 20)'}])
// MAGIC df.select(flatten_polygons('wkt')).show()

// COMMAND ----------

val df = List(("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))")).toDF("wkt")
df.select(flatten_polygons($"wkt")).show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT flatten_polygons("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))")

// COMMAND ----------

// MAGIC %md
// MAGIC ### point_index

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'lon': 30., 'lat': 10.}])
// MAGIC df.select(point_index('lat', 'lon', lit(10))).show(1, False)

// COMMAND ----------

val df = List((30.0, 10.0)).toDF("lon", "lat")
df.select(point_index($"lon", $"lat", lit(10))).show()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT point_index(30d, 10d, 10)

// COMMAND ----------

// MAGIC %md
// MAGIC ### polyfill

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))'}])
// MAGIC df.select(polyfill('wkt', lit(0))).show(2, False)

// COMMAND ----------

val df = List(("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))")).toDF("wkt")
df.select(polyfill($"wkt", lit(0))).show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT polyfill("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))", 0)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### mosaic_explode

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))'}])
// MAGIC df.select(mosaic_explode('wkt', lit(0))).show()

// COMMAND ----------

val df = List(("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))")).toDF("wkt")
df.select(mosaic_explode($"wkt", lit(0))).show()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT mosaic_explode("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))", 0)

// COMMAND ----------

// MAGIC %md
// MAGIC ### mosaicfill

// COMMAND ----------

// MAGIC %python
// MAGIC help(mosaicfill)

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))'}])
// MAGIC df.select(mosaicfill('wkt', lit(0))).show()

// COMMAND ----------

// MAGIC %python
// MAGIC df.select(mosaicfill('wkt', lit(0))).printSchema()

// COMMAND ----------

val df = List(("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))")).toDF("wkt")
df.select(mosaic_explode($"wkt", lit(0))).show()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT mosaic_explode("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))", 0)

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))'}])
// MAGIC df2 = df.select(mosaicfill('wkt', lit(0)))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Spatial predicates

// COMMAND ----------

// MAGIC %md 
// MAGIC ### st_contains

// COMMAND ----------

// MAGIC %python
// MAGIC help(st_contains)

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'point': 'POINT (25 15)', 'poly': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
// MAGIC df.select(st_contains('poly', 'point')).show()

// COMMAND ----------

val df = List(("POINT (25 15)", "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("point", "poly")
df.select(st_contains($"poly", $"point")).show()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_contains("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))", "POINT (25 15)")
