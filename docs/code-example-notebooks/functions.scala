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

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
// MAGIC showDF(select(df, st_area(column("wkt"))))

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

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
// MAGIC showDF(select(df, st_buffer(column("wkt"), lit(2))))

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

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
// MAGIC showDF(select(df, st_length(column("wkt"))))

// COMMAND ----------

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
// MAGIC showDF(select(df, st_perimeter(column("wkt"))))

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

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(wkt = "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"))
// MAGIC showDF(select(df, st_convexhull(column("wkt"))))

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

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(wkt = "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"))
// MAGIC showDF(select(df, st_dump(column("wkt"))))

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

// MAGIC %r
// MAGIC json_geom <- '{"type":"MultiPoint","coordinates":[[10,40],[40,30],[20,20],[30,10]],"crs":{"type":"name","properties":{"name":"EPSG:4326"}}}'
// MAGIC df <- createDataFrame(data.frame(json=json_geom))
// MAGIC showDF(select(df, st_srid(as_json(column('json')))))

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

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(wkt = "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"))
// MAGIC showDF(select(df, st_setsrid(st_geomfromwkt(column("wkt")), lit(4326L))))

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

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(wkt = "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"))
// MAGIC df <- withColumn(df, 'geom', st_setsrid(st_geomfromwkt(column('wkt')), lit(4326L)))
// MAGIC showDF(select(df, st_astext(st_transform(column('geom'), lit(3857L)))), truncate=F)

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

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(wkt = "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"))
// MAGIC showDF(select(df, st_translate(column('wkt'), lit(10), lit(-5))))

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

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
// MAGIC showDF(select(df, st_scale(column('wkt'), lit(0.5), lit(2))), truncate=F)

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

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
// MAGIC showDF(select(df, st_rotate(column("wkt"), lit(pi))), truncate=F)

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_centroid2D

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
// MAGIC df.select(st_centroid2D('wkt')).show()

// COMMAND ----------

val df = List(("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("wkt")
df.select(st_centroid2D($"wkt")).show()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_centroid2D("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")

// COMMAND ----------

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
// MAGIC showDF(select(df, st_centroid2D(column("wkt"))), truncate=F)

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_numpoints

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
// MAGIC df.select(st_numpoints('wkt')).show()

// COMMAND ----------

val df = List(("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("wkt")
df.select(st_numpoints($"wkt")).show()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_numpoints("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")

// COMMAND ----------

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
// MAGIC showDF(select(df, st_numpoints(column("wkt"))))

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

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
// MAGIC showDF(select(df, st_isvalid(column("wkt"))), truncate=F)

// COMMAND ----------

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(wkt = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))"))
// MAGIC showDF(select(df, st_isvalid(column("wkt"))), truncate=F)

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

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
// MAGIC showDF(select(df, st_geometrytype(column("wkt"))), truncate=F)

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

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
// MAGIC showDF(select(df, st_xmin(column("wkt"))), truncate=F)

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

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(point = c( "POINT (5 5)"), poly = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
// MAGIC showDF(select(df, st_distance(column("poly"), column("point"))))

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_intersection

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'p1': 'POLYGON ((0 0, 0 3, 3 3, 3 0))', 'p2': 'POLYGON ((2 2, 2 4, 4 4, 4 2))'}])
// MAGIC df.select(st_intersection(col('p1'), col('p2'))).show(1, False)

// COMMAND ----------

val df = List(("POLYGON ((0 0, 0 3, 3 3, 3 0))", "POLYGON ((2 2, 2 4, 4 4, 4 2))")).toDF("p1", "p2")
df.select(st_intersection($"p1", $"p2")).show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_intersection("POLYGON ((0 0, 0 3, 3 3, 3 0))", "POLYGON ((2 2, 2 4, 4 4, 4 2))")

// COMMAND ----------

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(p1 = "POLYGON ((0 0, 0 3, 3 3, 3 0))", p2 = "POLYGON ((2 2, 2 4, 4 4, 4 2))"))
// MAGIC showDF(select(df, st_intersection(column("p1"), column("p2"))), truncate=F)

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

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(wkt = 'MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))'))
// MAGIC showDF(select(df, flatten_polygons(column("wkt"))), truncate=F)

// COMMAND ----------

// MAGIC %md
// MAGIC ### point_index_lonlat

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'lon': 30., 'lat': 10.}])
// MAGIC df.select(point_index_lonlat('lon', 'lat', lit(10))).show(1, False)

// COMMAND ----------

val df = List((30.0, 10.0)).toDF("lon", "lat")
df.select(point_index_lonlat($"lon", $"lat", lit(10))).show()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT point_index_lonlat(30d, 10d, 10)

// COMMAND ----------

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(lon = 30.0, lat = 10.0))
// MAGIC showDF(select(df, point_index_lonlat(column("lon"), column("lat"), lit(10L))), truncate=F)

// COMMAND ----------

// MAGIC %md
// MAGIC ### point_index_geom

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'lon': 30., 'lat': 10.}])
// MAGIC df.select(point_index_lonlat(st_point('lon', 'lat'), lit(10))).show(1, False)

// COMMAND ----------

val df = List((30.0, 10.0)).toDF("lon", "lat")
df.select(point_index_lonlat(st_point($"lon", $"lat"), lit(10))).show()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT point_index_lonlat(st_point(30d, 10d), 10)

// COMMAND ----------

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(lon = 30.0, lat = 10.0))
// MAGIC showDF(select(df, point_index_lonlat(st_point(column("lon"), column("lat")), lit(10L))), truncate=F)

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

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(wkt = "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))"))
// MAGIC showDF(select(df, polyfill(column("wkt"), lit(0L))), truncate=F)

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

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(wkt = 'MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))'))
// MAGIC showDF(select(df, mosaic_explode(column("wkt"), lit(0L))))

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

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(wkt = "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))"))
// MAGIC schema(select(df, mosaicfill(column("wkt"), lit(0L))))
// MAGIC showDF(select(df, mosaicfill(column("wkt"), lit(0L))))
