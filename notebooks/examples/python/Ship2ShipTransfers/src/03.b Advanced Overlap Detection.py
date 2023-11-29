# Databricks notebook source
# MAGIC %md ## Line Aggregation
# MAGIC
# MAGIC > Instead of the point-to-point evaluation, we will instead be aggregating into lines and comparing as such.
# MAGIC
# MAGIC ---
# MAGIC __Last Updated:__ 27 NOV 2023 [Mosaic 0.3.12]

# COMMAND ----------

# MAGIC %md ## Setup

# COMMAND ----------

# MAGIC %pip install "databricks-mosaic<0.4,>=0.3" --quiet # <- Mosaic 0.3 series
# MAGIC # %pip install "databricks-mosaic<0.5,>=0.4" --quiet # <- Mosaic 0.4 series (as available)

# COMMAND ----------

# -- configure AQE for more compute heavy operations
#  - choose option-1 or option-2 below, essential for REPARTITION!
# spark.conf.set("spark.databricks.optimizer.adaptive.enabled", False) # <- option-1: turn off completely for full control
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False) # <- option-2: just tweak partition management
spark.conf.set("spark.sql.shuffle.partitions", 1_024)                  # <-- default is 200

# -- import databricks + spark functions
from pyspark.databricks.sql import functions as dbf
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import *

# -- setup mosaic
import mosaic as mos

mos.enable_mosaic(spark, dbutils)
# mos.enable_gdal(spark) # <- not needed for this example

# --other imports
import warnings

warnings.simplefilter("ignore")

# COMMAND ----------

# MAGIC %md __Configure Database__
# MAGIC
# MAGIC > Adjust this to settings from the Data Prep notebook.

# COMMAND ----------

catalog_name = "mjohns"
db_name = "ship2ship"

sql(f"use catalog {catalog_name}")
sql(f"use schema {db_name}")

# COMMAND ----------

cargos_indexed = spark.read.table("cargos_indexed")
        
print(f"count? {cargos_indexed.count():,}")
cargos_indexed.limit(5).display() # <- limiting for ipynb only

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

spark.catalog.clearCache()                       # <- cache is useful for dev (avoid recompute)
lines = (
    cargos_indexed
        .repartition(sc.defaultParallelism * 20) # <- repartition is important!
        .groupBy("mmsi", F.window("BaseDateTime", "15 minutes"))
            # We link the points to their respective timestamps in the aggregation
            .agg(F.collect_list(F.struct(col("point_geom"), col("BaseDateTime"))).alias("coords"))
        # And then sort our array of points by the timestamp to form a trajectory
        .withColumn(
            "coords",
            F.expr(
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
print(f"count? {lines.count():,}")

# COMMAND ----------

# MAGIC %md _Note here that this decreases the total number of rows across which we are running our comparisons._

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
    max_np = lines.select(F.max(np)).collect()[0][0]
    return F.lit(max_np) * F.lit(buffer) / np


cargo_movement = (
    lines.withColumn("buffer_r", get_buffer("line"))
    .withColumn("buffer_geom", mos.st_buffer("line", col("buffer_r")))
    .withColumn("buffer", mos.st_astext("buffer_geom"))
    .withColumn("ix", mos.grid_tessellateexplode("buffer_geom", F.lit(9)))
)

cargo_movement.createOrReplaceTempView("ship_path") # <- create a temp view
spark.read.table("ship_path").limit(1).display()    # <- limiting for ipynb only

# COMMAND ----------

to_plot = spark.read.table("ship_path").select("buffer").limit(3_000).distinct()

# COMMAND ----------

# MAGIC %md _Example buffer paths_

# COMMAND ----------

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
    .saveAsTable("overlap_candidates_lines")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM overlap_candidates_lines LIMIT 1

# COMMAND ----------

# MAGIC %md _We can show the most common locations for overlaps happening, as well some example ship paths during those overlaps._

# COMMAND ----------

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

harbours_h3 = spark.read.table("harbours_h3")
candidates = spark.read.table("overlap_candidates_lines")

# COMMAND ----------

matches = (
    candidates.join(
        harbours_h3, how="leftanti", on=candidates["index"] == harbours_h3["h3"]
    )
    .groupBy("vessel_1", "vessel_2")
    .agg(F.first("line_1").alias("line_1"), F.first("line_2").alias("line_2"))
)

# COMMAND ----------

(
    matches.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("overlap_candidates_lines_filtered")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT format_number(COUNT(1),0) FROM overlap_candidates_lines_filtered;

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC overlap_candidates_lines_filtered "line_1" "geometry" 2_000
