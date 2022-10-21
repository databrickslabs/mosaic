from pyspark.sql import SparkSession, DataFrame, SQLContext

class ApproximateSpatialKNN:
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.model = getattr(
            self.spark._jvm.com.databricks.labs.mosaic.models, "ApproximateSpatialKNN"
        )()

    def setKNeighbours(self, k):
        self.model.setKNeighbours(k)
        return self

    def setLeftFeatureCol(self, feature):
        self.model.setLeftFeatureCol(feature)
        return self

    def setRightFeatureCol(self, feature):
        self.model.setRightFeatureCol(feature)
        return self

    def setMaxIterations(self, n):
        self.model.setMaxIterations(n)
        return self

    def setEarlyStopping(self, n):
        self.model.setEarlyStopping(n)
        return self

    def setDistanceThreshold(self, n):
        self.model.setDistanceThreshold(n)
        return self

    def setCheckpointTablePrefix(self, prefix):
        self.model.setCheckpointTablePrefix(prefix)
        return self

    def setIndexResolution(self, resolution):
        self.model.setIndexResolution(resolution)
        return self

    def setRightDf(self, df):
        self.model.setRightDf(df._jdf)
        return self

    def transform(self, df):
        result = self.model.transform(df._jdf)
        return DataFrame(result, SQLContext(self.spark.sparkContext))