# Databricks notebook source
# MAGIC %md ## Line Aggregation
# MAGIC
# MAGIC Instead of the point-to-point evaluation, we will instead be aggregating into lines and comparing as such.

# COMMAND ----------

# MAGIC %pip install databricks_mosaic

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
# MAGIC Since our projection is not in metres, we convert from decimal degrees. With `(0.00001 - 0.000001)` as being equal to one metre at the equator
# MAGIC Ref: http://wiki.gis.com/wiki/index.php/Decimal_degrees

# COMMAND ----------

lines = (
    cargos_indexed.repartition(sc.defaultParallelism * 20)
    .groupBy("mmsi", window("BaseDateTime", "15 minutes"))
    # We link the points to their respective timestamps in the aggregation
    .agg(collect_list(struct(col("point_geom"), col("BaseDateTime"))).alias("coords"))
    # And then sort our array of points by the timestamp to form a trajectory
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
    The intuition behind this choice is held in the way AIS positions are emitted. The faster the vessel moves the
    more positions it will emit — yielding a smoother trajectory, where slower vessels will yield far fewer positions
    and a harder to reconstruct trajectory which inherently holds more uncertainty.

    Args:
        line (geometry): linestring geometry as generated with st_makeline.

    Returns:
        double: buffer size in degrees
    """
    np = mos.st_numpoints(line)
    max_np = lines.select(max(np)).collect()[0][0]
    return lit(max_np) * lit(buffer) / np


cargo_movement = (
    lines.withColumn("buffer_r", get_buffer("line"))
    .withColumn("buffer_geom", mos.st_buffer("line", col("buffer_r")))
    .withColumn("buffer", mos.st_astext("buffer_geom"))
    .withColumn("ix", mos.grid_tessellateexplode("buffer_geom", lit(9)))
)

(cargo_movement.createOrReplaceTempView("ship_path"))

display(spark.read.table("ship_path"))

# COMMAND ----------

to_plot = spark.read.table("ship_path").select("buffer").limit(3_000).distinct()

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
            col("a.ix.index_id")
            == col("b.ix.index_id"),  # to only compare across efficient indices
            col("a.mmsi")
            < col("b.mmsi"),  # to prevent comparing candidates bidirectionally
            col("a.window")
            == col("b.window"),  # to compare across the same time window
        ],
    )
    .where(
        (
            col("a.ix.is_core") | col("b.ix.is_core")
        )  # if either candidate fully covers an index, no further comparison is needed
        | mos.st_intersects(
            "a.ix.wkb", "b.ix.wkb"
        )  # limit geospatial querying to cases where indices alone cannot give certainty
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
    candidates.join(
        harbours_h3, how="leftanti", on=candidates["index"] == harbours_h3["h3"]
    )
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
