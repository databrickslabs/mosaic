# Databricks notebook source
# MAGIC %md # Overlap Detection
# MAGIC
# MAGIC > We now try to detect potentially overlapping pings using a buffer on a particular day.
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
import os
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

cargos_indexed = spark.read.table("cargos_indexed").filter(
    col("BaseDateTime").between(
        "2018-01-31T00:00:00.000+0000", "2018-01-31T23:59:00.000+0000"
    )
)
print(f"count? {cargos_indexed.count():,}")
cargos_indexed.limit(5).display() # <- limiting for ipynb only

# COMMAND ----------

# MAGIC %md ## Buffering
# MAGIC
# MAGIC <p/>
# MAGIC
# MAGIC 1. Convert the point into a polygon by buffering it with a certain area to turn this into a circle.
# MAGIC 2. Index the polygon to leverage more performant querying.
# MAGIC
# MAGIC > Since our projection is not in metres, we convert from decimal degrees, with `(0.00001 - 0.000001)` as being equal to one metre at the equator. Here we choose an buffer of roughly 100 metres, ref http://wiki.gis.com/wiki/index.php/Decimal_degrees.

# COMMAND ----------

one_metre = 0.00001 - 0.000001
buffer = 100 * one_metre

(
    cargos_indexed
    .repartition(sc.defaultParallelism * 20) # <- repartition is important!
    .withColumn("buffer_geom", mos.st_buffer("point_geom", F.lit(buffer)))
    .withColumn("ix", mos.grid_tessellateexplode("buffer_geom", F.lit(9)))
    .write.mode("overwrite")
    .saveAsTable("cargos_buffered")
)

# COMMAND ----------

# MAGIC %md _We will optimise our table to colocate data and make querying faster._
# MAGIC
# MAGIC > This is showing [ZORDER](https://docs.databricks.com/en/delta/data-skipping.html); for newer runtimes (DBR 13.3 LTS+), can also consider [Liquid Clustering](https://docs.databricks.com/en/delta/clustering.html).

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE cargos_buffered ZORDER BY (ix.index_id, BaseDateTime);
# MAGIC SELECT * FROM cargos_buffered LIMIT 5;

# COMMAND ----------

# MAGIC %md ## Implement Algorithm

# COMMAND ----------

buffered_events = (
    spark.read.table("cargos_buffered")
        .repartition(sc.defaultParallelism * 20) # <- repartition is important!
)

def ts_diff(ts1, ts2):
    """Output the difference between two timestamps in seconds.

    Args:
        ts1 (Timestamp): First Timestamp
        ts2 (Timestamp): Second Timestamp

    Returns:
        long: The difference between two timestamps in seconds.
    """
    return F.abs(col(ts1).cast("long") - col(ts2).cast("long"))


def time_window(sog1, sog2, heading1, heading2, radius):
    """Create dynamic time window based on speed, buffer radius and heading.

    Args:
        sog1 (double): vessel 1's speed over ground, in knots
        sog2 (double): vessel 2's speed over ground, in knots
        heading1 (double): vessel 1's heading angle in degrees
        heading2 (double): vessel 2's heading angle in degrees
        radius (double): buffer radius in degrees

    Returns:
        double: dynamic time window in seconds based on the speed and radius
    """
    v_x1 = col(sog1) * F.cos(col(heading1))
    v_y1 = col(sog1) * F.sin(col(heading1))
    v_x2 = col(sog2) * F.cos(col(heading2))
    v_y2 = col(sog2) * F.sin(col(heading2))

    # compute relative vectors speed based x and y partial speeds
    v_relative = F.sqrt((v_x1 + v_x2) * (v_x1 + v_x2) + (v_y1 + v_y2) * (v_y1 + v_y2))
    # convert to m/s and determine ratio between speed and radius
    return v_relative * F.lit(1000) / F.lit(radius) / F.lit(3600)


candidates = (
    buffered_events.alias("a")
    .join(
        buffered_events.alias("b"),
        [
            col("a.ix.index_id")
            == col("b.ix.index_id"),  # to only compare across efficient indices
            col("a.mmsi")
            < col("b.mmsi"),  # to prevent comparing candidates bidirectionally
            ts_diff("a.BaseDateTime", "b.BaseDateTime")
            < time_window("a.sog_kmph", "b.sog_kmph", "a.heading", "b.heading", buffer),
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
        col("a.vesselName").alias("vessel_1"),
        col("b.vesselName").alias("vessel_2"),
        col("a.BaseDateTime").alias("timestamp_1"),
        col("b.BaseDateTime").alias("timestamp_2"),
        col("a.ix.index_id").alias("ix"),
    )
)

# COMMAND ----------

(candidates.write.mode("overwrite").saveAsTable("overlap_candidates"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM overlap_candidates limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW agg_overlap AS
# MAGIC SELECT ix, count(*) AS count
# MAGIC FROM overlap_candidates
# MAGIC GROUP BY ix, vessel_1, vessel_2
# MAGIC ORDER BY count DESC;
# MAGIC
# MAGIC SELECT * FROM agg_overlap LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Plotting Common Overlaps
# MAGIC %%mosaic_kepler
# MAGIC "agg_overlap" "ix" "h3" 10_000
