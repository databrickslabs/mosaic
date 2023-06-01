from pyspark.sql import SparkSession

from .mosaic_data_frame_reader import MosaicDataFrameReader


def read():
    """
    Returns a MosaicDataFrameReader for reading MosaicDataFrames.
    """
    spark = SparkSession.builder.getOrCreate()
    return MosaicDataFrameReader()
