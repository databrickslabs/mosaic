// Databricks notebook source
// MAGIC %md
// MAGIC # Code examples for Mosaic documentation

// COMMAND ----------

// MAGIC %md
// MAGIC ## Setup

// COMMAND ----------

// MAGIC %run ./setup/setup-python

// COMMAND ----------

// MAGIC %run ./setup/setup-scala

// COMMAND ----------

// MAGIC %run ./setup/setup-r

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

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame('wkt'= "POINT (30 10)"))
// MAGIC showDF(select(df, alias(st_asbinary(column("wkt")), "wkb")))

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

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame('wkt'= "POINT (30 10)"))
// MAGIC showDF(select(df, alias(st_asgeojson(column("wkt")), "json")), truncate=F)

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

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(lon = 30.0, lat = 10.0))
// MAGIC showDF(select(df, alias(st_aswkt(st_point(column("lon"), column("lat"))), "wkt")), truncate=F)
