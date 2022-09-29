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
// MAGIC ## Spatial aggregation functions

// COMMAND ----------

// MAGIC %md 
// MAGIC ### st_intersection_aggregate

// COMMAND ----------

// MAGIC %python
// MAGIC left_df = (
// MAGIC   spark.createDataFrame([{'geom': 'POLYGON ((0 0, 0 3, 3 3, 3 0))'}])
// MAGIC   .select(grid_tessellateexplode(col("geom"), lit(1)).alias("left_index"))
// MAGIC )
// MAGIC right_df = (
// MAGIC   spark.createDataFrame([{'geom': 'POLYGON ((2 2, 2 4, 4 4, 4 2))'}])
// MAGIC   .select(grid_tessellateexplode(col("geom"), lit(1)).alias("right_index"))
// MAGIC )
// MAGIC (
// MAGIC   left_df
// MAGIC   .join(right_df, col("left_index.index_id") == col("right_index.index_id"))
// MAGIC   .groupBy()
// MAGIC   .agg(st_astext(st_intersection_aggregate(col("left_index"), col("right_index"))))
// MAGIC ).show(1, False)

// COMMAND ----------

val leftDf = List("POLYGON ((0 0, 0 3, 3 3, 3 0))").toDF("geom")
  .select(grid_tessellateexplode($"geom", lit(1)).alias("left_index"))
val rightDf = List("POLYGON ((2 2, 2 4, 4 4, 4 2))").toDF("geom")
  .select(grid_tessellateexplode($"geom", lit(1)).alias("right_index"))
leftDf
  .join(rightDf, $"left_index.index_id" === $"right_index.index_id")
  .groupBy()
  .agg(st_astext(st_intersection_aggregate($"left_index", $"right_index")))
  .show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC WITH l AS (SELECT grid_tessellateexplode("POLYGON ((0 0, 0 3, 3 3, 3 0))", 1) AS left_index),
// MAGIC r AS (SELECT grid_tessellateexplode("POLYGON ((2 2, 2 4, 4 4, 4 2))", 1) AS right_index)
// MAGIC SELECT st_astext(st_intersection_aggregate(l.left_index, r.right_index))
// MAGIC FROM l INNER JOIN r on l.left_index.index_id = r.right_index.index_id

// COMMAND ----------

// MAGIC %r
// MAGIC df.l <- select(
// MAGIC   createDataFrame(data.frame(geom = "POLYGON ((0 0, 0 3, 3 3, 3 0))")), 
// MAGIC   alias(grid_tessellateexplode(column("geom"), lit(1L)), "left_index")
// MAGIC )
// MAGIC df.r <- select(
// MAGIC   createDataFrame(data.frame(geom = "POLYGON ((2 2, 2 4, 4 4, 4 2))")),
// MAGIC   alias(grid_tessellateexplode(column("geom"), lit(1L)), "right_index")
// MAGIC )
// MAGIC showDF(
// MAGIC   select(
// MAGIC     join(df.l, df.r, df.l$left_index.index_id == df.r$right_index.index_id),
// MAGIC     st_astext(st_intersection_aggregate(column("left_index"), column("right_index")))
// MAGIC   ), truncate=F
// MAGIC )

// COMMAND ----------

// MAGIC %md 
// MAGIC ### st_intersects_aggregate

// COMMAND ----------

// MAGIC %python
// MAGIC left_df = (
// MAGIC   spark.createDataFrame([{'geom': 'POLYGON ((0 0, 0 3, 3 3, 3 0))'}])
// MAGIC   .select(grid_tessellateexplode(col("geom"), lit(1)).alias("left_index"))
// MAGIC )
// MAGIC right_df = (
// MAGIC   spark.createDataFrame([{'geom': 'POLYGON ((2 2, 2 4, 4 4, 4 2))'}])
// MAGIC   .select(grid_tessellateexplode(col("geom"), lit(1)).alias("right_index"))
// MAGIC )
// MAGIC (
// MAGIC   left_df
// MAGIC   .join(right_df, col("left_index.index_id") == col("right_index.index_id"))
// MAGIC   .groupBy()
// MAGIC   .agg(st_intersects_aggregate(col("left_index"), col("right_index")))
// MAGIC ).show(1, False)

// COMMAND ----------

val leftDf = List("POLYGON ((0 0, 0 3, 3 3, 3 0))").toDF("geom")
  .select(grid_tessellateexplode($"geom", lit(1)).alias("left_index"))
val rightDf = List("POLYGON ((2 2, 2 4, 4 4, 4 2))").toDF("geom")
  .select(grid_tessellateexplode($"geom", lit(1)).alias("right_index"))
leftDf
  .join(rightDf, $"left_index.index_id" === $"right_index.index_id")
  .groupBy()
  .agg(st_intersects_aggregate($"left_index", $"right_index"))
  .show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC WITH l AS (SELECT grid_tessellateexplode("POLYGON ((0 0, 0 3, 3 3, 3 0))", 1) AS left_index),
// MAGIC r AS (SELECT grid_tessellateexplode("POLYGON ((2 2, 2 4, 4 4, 4 2))", 1) AS right_index)
// MAGIC SELECT st_intersects_aggregate(l.left_index, r.right_index)
// MAGIC FROM l INNER JOIN r on l.left_index.index_id = r.right_index.index_id

// COMMAND ----------

// MAGIC %r
// MAGIC df.l <- select(
// MAGIC   createDataFrame(data.frame(geom = "POLYGON ((0 0, 0 3, 3 3, 3 0))")), 
// MAGIC   alias(grid_tessellateexplode(column("geom"), lit(1L)), "left_index")
// MAGIC )
// MAGIC df.r <- select(
// MAGIC   createDataFrame(data.frame(geom = "POLYGON ((2 2, 2 4, 4 4, 4 2))")),
// MAGIC   alias(grid_tessellateexplode(column("geom"), lit(1L)), "right_index")
// MAGIC )
// MAGIC showDF(
// MAGIC   select(
// MAGIC     join(df.l, df.r, df.l$left_index.index_id == df.r$right_index.index_id),
// MAGIC     st_intersects_aggregate(column("left_index"), column("right_index"))
// MAGIC   ), truncate=F
// MAGIC )
