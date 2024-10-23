from typing import *

from pyspark.sql import DataFrame, SparkSession, SQLContext


class MosaicAnalyzer:
    """
    MosaicAnalyzer is a class that provides the ability to analyze spatial data
    and provide insights into the optimal resolution for the given dataset.
    This only works for geometries that have area > 0.
    """

    def __init__(self, dataframe: DataFrame):
        """
        Initialize the SpatialKNN model.
        """

        self.spark = SparkSession.builder.getOrCreate()
        self.model = getattr(
            self.spark._jvm.com.databricks.labs.mosaic.sql, "MosaicAnalyzer"
        )(dataframe._jdf)

    def get_optimal_resolution(self, geometry_column: str):
        """
        Get the optimal resolution for the given dataset.
        """
        return self.model.getOptimalResolution(geometry_column)

    def get_optimal_resolution(self, geometry_column: str, nrows: int):
        """
        Get the optimal resolution for the given dataset.
        """
        return self.model.getOptimalResolution(geometry_column, nrows)

    def get_optimal_resolution(self, geometry_column: str, sample: float):
        """
        Get the optimal resolution for the given dataset.
        """
        return self.model.getOptimalResolution(geometry_column, sample)
