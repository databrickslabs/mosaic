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

// COMMAND ----------

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(point = c( "POINT (25 15)"), poly = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
// MAGIC showDF(select(df, st_contains(column("poly"), column("point"))))

// COMMAND ----------

// MAGIC %md
// MAGIC ### st_intersects

// COMMAND ----------

// MAGIC %python
// MAGIC help(st_intersects)

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.createDataFrame([{'p1': 'POLYGON ((0 0, 0 3, 3 3, 3 0))', 'p2': 'POLYGON ((2 2, 2 4, 4 4, 4 2))'}])
// MAGIC df.select(st_intersects(col('p1'), col('p2'))).show(1, False)

// COMMAND ----------

val df = List(("POLYGON ((0 0, 0 3, 3 3, 3 0))", "POLYGON ((2 2, 2 4, 4 4, 4 2))")).toDF("p1", "p2")
df.select(st_intersects($"p1", $"p2")).show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT st_intersects("POLYGON ((0 0, 0 3, 3 3, 3 0))", "POLYGON ((2 2, 2 4, 4 4, 4 2))")

// COMMAND ----------

// MAGIC %r
// MAGIC df <- createDataFrame(data.frame(p1 = "POLYGON ((0 0, 0 3, 3 3, 3 0))", p2 = "POLYGON ((2 2, 2 4, 4 4, 4 2))"))
// MAGIC showDF(select(df, st_intersects(column("p1"), column("p2"))), truncate=F)
