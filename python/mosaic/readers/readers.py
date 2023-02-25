from .mosaic_data_frame_reader import MosaicDataFrameReader
from pyspark.sql import SparkSession

def read():
    """
    Returns a MosaicDataFrameReader for reading MosaicDataFrames.
    """
    spark = SparkSession.builder.getOrCreate()
    return MosaicDataFrameReader(spark)