from pyspark.sql import functions as f
from pyspark.sql import SparkSession

def register(_spark):
    # register functions via the reader
    _spark = SparkSession.builder.getOrCreate()
    _spark.read.format("register_ds").option("functions", "vectorx.jts.legacy").load().collect()


def st_legacyaswkb(geom):
    return f.call_function("gbx_st_legacyaswkb", geom)
