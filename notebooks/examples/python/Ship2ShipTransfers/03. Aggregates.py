# Databricks notebook source
# MAGIC %md ## Line Aggregation
# MAGIC
# MAGIC Instead of the point-to-point evaluation, we will instead be aggregating into lines and comparing as such.

# COMMAND ----------

from pyspark.sql.functions import *
import mosaic as mos

spark.conf.set("spark.databricks.labs.mosaic.geometry.api", "ESRI")
spark.conf.set("spark.databricks.labs.mosaic.index.system", "H3")
mos.enable_mosaic(spark, dbutils)

# COMMAND ----------

cargos_indexed = spark.read.table("ship2ship.cargos_indexed").repartition(
    sc.defaultParallelism * 20
)
display(cargos_indexed)
cargos_indexed.count()

# COMMAND ----------

# MAGIC %md ## Create Lines
# MAGIC
# MAGIC We can `groupBy` across a timewindow to give us aggregated geometries to work with.
# MAGIC
# MAGIC When we collect the various points within a timewindow, we want to construct the linestring by the order in which they were generated (timestamp).
# MAGIC We choose a buffer of a max of 200 metres in this case.

# COMMAND ----------

lines = (
    cargos_indexed.repartition(sc.defaultParallelism * 20)
    .groupBy("mmsi", window("BaseDateTime", "15 minutes"))
    # We link the points to their respective timestamps in the aggregation
    .agg(collect_list(struct(col("point_geom"), col("BaseDateTime"))).alias("coords"))
    # And then sort our array of points by the timestamp
    .withColumn(
        "coords",
        expr(
            """
        array_sort(coords, (left, right) -> 
            case 
            when left.BaseDateTime < right.BaseDateTime then -1 
            when left.BaseDateTime > right.BaseDateTime then 1 
            else 0 
            end
        )"""
        ),
    )
    .withColumn("line", mos.st_makeline(col("coords.point_geom")))
    .cache()
)

# COMMAND ----------

# DBTITLE 1,Note here that this decreases the total number of rows across which we are running our comparisons.
lines.count()

# COMMAND ----------


one_metre = 0.00001 - 0.000001
buffer = 200 * one_metre


def get_buffer(line, buffer=buffer):
    """Create buffer as function of number of points in linestring
    The buffer size is inversely proportional to the number of points, providing a larger buffer for slower ships.

    Args:
        line (geometry): linestring geometry as generated with st_makeline.

    Returns:
        double: buffer size in degrees
    """
    np = expr(f"st_numpoints({line})")
    max_np = lines.select(max(np)).collect()[0][0]
    return lit(max_np) * lit(buffer) / np


cargo_movement = (
    lines.withColumn("buffer_r", get_buffer("line"))
    .withColumn("buffer_geom", mos.st_buffer("line", col("buffer_r")))
    .withColumn("buffer", mos.st_astext("buffer_geom"))
    .withColumn("ix", mos.mosaic_explode("buffer_geom", lit(9)))
)

(cargo_movement.createOrReplaceTempView("ship_path"))

display(spark.read.table("ship_path"))

# COMMAND ----------

to_plot = spark.read.table("ship_path").select("buffer").limit(3_000)

# COMMAND ----------

# DBTITLE 1,Example Buffer Paths
# MAGIC %%mosaic_kepler
# MAGIC to_plot "buffer" "geometry" 3_000

# COMMAND ----------

# MAGIC %md ## Find All Candidates
# MAGIC
# MAGIC We employ a join strategy using Mosaic indices as before, but this time we leverage the buffered ship paths.

# COMMAND ----------

candidates_lines = (
    cargo_movement.alias("a")
    .join(
        cargo_movement.alias("b"),
        [
            col("a.ix.index_id") == col("b.ix.index_id"),
            col("a.mmsi") < col("b.mmsi"),
            col("a.window") == col("b.window"),
        ],
    )
    .where(
        (col("a.ix.is_core") | col("b.ix.is_core"))
        | mos.st_intersects("a.ix.wkb", "b.ix.wkb")
    )
    .select(
        col("a.mmsi").alias("vessel_1"),
        col("b.mmsi").alias("vessel_2"),
        col("a.window").alias("window"),
        col("a.buffer").alias("line_1"),
        col("b.buffer").alias("line_2"),
        col("a.ix.index_id").alias("index"),
    )
    .drop_duplicates()
)

(
    candidates_lines.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("ship2ship.overlap_candidates_lines")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ship2ship.overlap_candidates_lines;

# COMMAND ----------

# DBTITLE 1,We can show the most common locations for overlaps happening, as well some example ship paths during those overlaps.
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW agg_overlap AS
# MAGIC SELECT index AS ix, count(*) AS count, FIRST(line_1) AS line_1, FIRST(line_2) AS line_2
# MAGIC FROM ship2ship.overlap_candidates_lines
# MAGIC GROUP BY ix
# MAGIC ORDER BY count DESC

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC "agg_overlap" "ix" "h3" 2_000

# COMMAND ----------

# MAGIC %md ## Filtering Out Harbours
# MAGIC In the data we see many overlaps near harbours. We can reasonably assume that these are overlaps due to being in close proximity of the harbour, not a transfer.
# MAGIC Therefore, we can filter those out below.

# COMMAND ----------

harbours_h3 = spark.read.table("ship2ship.harbours_h3")
candidates = spark.read.table("ship2ship.overlap_candidates_lines")

# COMMAND ----------

matches = (
    candidates_lines.join(
        harbours_h3, how="leftouter", on=candidates_lines["index"] == harbours_h3["h3"]
    )
    .where(harbours_h3["h3"].isNull())
    .groupBy("vessel_1", "vessel_2")
    .agg(first("line_1").alias("line_1"), first("line_2").alias("line_2"))
)

# COMMAND ----------

(
    matches.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("ship2ship.overlap_candidates_lines_filtered")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM ship2ship.overlap_candidates_lines_filtered;

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC ship2ship.overlap_candidates_lines_filtered "line_1" "geometry" 2_000
