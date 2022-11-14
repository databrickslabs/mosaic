from pyspark.sql import SparkSession, DataFrame, SQLContext
from mosaic.utils import scala_utils

class SpatialKNN:
    def __init__(self, right_df):
        self.spark = SparkSession.builder.getOrCreate()
        self.model = getattr(
            self.spark._jvm.com.databricks.labs.mosaic.models.knn, "SpatialKNN"
        )()
        self.model.setRightDf(right_df._jdf)

    def setUseTableCheckpoint(self, useTableCheckpoint):
        self.model.setUseTableCheckpoint(useTableCheckpoint)
        return self

    def approximate(self, approximate):
        self.model.setApproximate(approximate)
        return self

    def setLeftFeatureCol(self, feature):
        self.model.setLeftFeatureCol(feature)
        return self
        
    def setLeftRowID(self, rowID):
        self.model.setLeftRowID(rowID)
        return self
        
    def setRightFeatureCol(self, feature):
        self.model.setRightFeatureCol(feature)
        return self
        
    def setRightRowID(self, rowID):
        self.model.setRightRowID(rowID)
        return self
        
    def setDistanceThreshold(self, n):
        self.model.setDistanceThreshold(n)
        return self
        
    def setIndexResolution(self, resolution):
        self.model.setIndexResolution(resolution)
        return self

    def setKNeighbours(self, k):
        self.model.setKNeighbours(k)
        return self

    def setMaxIterations(self, n):
        self.model.setMaxIterations(n)
        return self

    def setEarlyStopping(self, n):
        self.model.setEarlyStopping(n)
        return self

    def setCheckpointTablePrefix(self, prefix):
        self.model.setCheckpointTablePrefix(prefix)
        return self

    def setRightDf(self, df):
        self.model.setRightDf(df._jdf)
        return self

    def transform(self, df):
        result = self.model.transform(df._jdf)
        return DataFrame(result, SQLContext(self.spark.sparkContext))

    def getParams(self):
        params = self.model.getParams()
        return scala_utils.scala_map_to_python(params)

    def getMetrics(self):
        metrics = self.model.getMetrics()
        return scala_utils.scala_map_to_python(metrics)

    def write(self):
        return self.model.write

    def load(self):
        return self.model.load