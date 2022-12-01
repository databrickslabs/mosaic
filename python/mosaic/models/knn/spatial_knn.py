from pyspark.sql import SparkSession, DataFrame, SQLContext
from mosaic.utils import scala_utils

class SpatialKNN:

    """
    SpatialKNN is a distributed KNN model that uses a spatial index to reduce the number of candidate records to
    consider for each query record. The model is built on top of the Spark DataFrame API and is designed to be
    used in a distributed fashion. The model uses hex rings as the means to iterate through the spatial grid and
    find the nearest neighbours. Delta tables are used to checkpoint the intermediate results of the model.
    The number of iterations is controlled using max number of iterations and early stopping parameters.
    """

    def __init__(self):
        """
        Initialize the SpatialKNN model.
        """

        self.spark = SparkSession.builder.getOrCreate()
        self.model = getattr(
            self.spark._jvm.com.databricks.labs.mosaic.models.knn, "SpatialKNN"
        )()

    def setUseTableCheckpoint(self, useTableCheckpoint):
        """
        Set whether to use a Delta table to checkpoint the intermediate results of the model.
        :param useTableCheckpoint:
            True to use a Delta table (provided as a table name) to checkpoint the intermediate results of the model.
            False to use a Delta table (provided as a file system path) to checkpoint the intermediate results of the model.
        :return: SpatialKNN model with the param set to provided value.
        """

        self.model.setUseTableCheckpoint(useTableCheckpoint)
        return self

    def setApproximate(self, approximate):
        """
        Set whether to use approximate nearest neighbours or to use exact nearest neighbours finalisation.
        :param approximate:
            True to use approximate nearest neighbours.
            False to use exact nearest neighbours finalisation.
        :return: SpatialKNN model with the param set to provided value.
        """

        self.model.setApproximate(approximate)
        return self

    def setLandmarksFeatureCol(self, feature):
        """
        Set the feature column name for the landmarks.
        :param feature: Feature column name for the landmarks.
        :return: SpatialKNN model with the param set to provided value.
        """

        self.model.setLandmarksFeatureCol(feature)
        return self
        
    def setLandmarksRowID(self, rowID):
        """
        Set the row ID column name for the landmarks.
        :param rowID: Row ID column name for the landmarks.
        :return: SpatialKNN model with the param set to provided value.
        """

        self.model.setLandmarksRowID(rowID)
        return self
        
    def setCandidatesFeatureCol(self, feature):
        """
        Ser the feature column name for the candidates.
        :param feature: Feature column name for the candidates.
        :return: SpatialKNN model with the param set to provided value.
        """

        self.model.setCandidatesFeatureCol(feature)
        return self
        
    def setCandidatesRowID(self, rowID):
        """
        Set the row ID column name for the candidates.
        :param rowID: Row ID column name for the candidates.
        :return: SpatialKNN model with the param set to provided value.
        """

        self.model.setCandidatesRowID(rowID)
        return self
        
    def setDistanceThreshold(self, d):
        """
        Set the distance threshold for the nearest neighbours.
        :param n: Distance threshold for the nearest neighbours.
        :return: SpatialKNN model with the param set to provided value.
        """

        self.model.setDistanceThreshold(d)
        return self
        
    def setIndexResolution(self, resolution):
        """
        Set the index resolution for the spatial index.
        :param resolution: Index resolution for the spatial index.
        :return: SpatialKNN model with the param set to provided value.
        """

        self.model.setIndexResolution(resolution)
        return self

    def setKNeighbours(self, k):
        """
        Set the number of nearest neighbours to find.
        :param k: Number of nearest neighbours to find.
        :return: SpatialKNN model with the param set to provided value.
        """

        self.model.setKNeighbours(k)
        return self

    def setMaxIterations(self, n):
        """
        Set the maximum number of iterations to run the model.
        :param n: Maximum number of iterations to run the model.
        :return: SpatialKNN model with the param set to provided value.
        """

        self.model.setMaxIterations(n)
        return self

    def setEarlyStopIterations(self, n):
        """
        Set the number of iterations to run before early stopping.
        :param n: Number of iterations to run before early stopping.
        :return: SpatialKNN model with the param set to provided value.
        """

        self.model.setEarlyStopIterations(n)
        return self

    def setCheckpointTablePrefix(self, prefix):
        """
        Set the checkpoint table prefix for the model.
        :param prefix: Checkpoint table prefix for the model.
        :return: SpatialKNN model with the param set to provided value.
        """

        self.model.setCheckpointTablePrefix(prefix)
        return self

    def setCandidatesDf(self, df):
        """
        Set candidates DataFrame.
        :param df: Candidates DataFrame.
        :return: SpatialKNN model with the param set to provided value.
        """

        self.model.setCandidatesDf(df._jdf)
        return self

    def transform(self, df):
        """
        Transform the provided DataFrame using the model.
        :param df: DataFrame to transform.
        :return: Transformed DataFrame.
        """

        result = self.model.transform(df._jdf)
        return DataFrame(result, SQLContext(self.spark.sparkContext))

    def getParams(self):
        """
        Get the parameters of the model.
        :return: Parameters of the model as a dict.
        """

        params = self.model.getParams()
        return scala_utils.scala_map_to_dict(params)

    def getMetrics(self):
        """
        Get the metrics of the model.
        :return: Metrics of the model as a dict.
        """

        metrics = self.model.getMetrics()
        return scala_utils.scala_map_to_dict(metrics)

    def write(self):
        """
        return: a SpatialKNNWriter instance for this SpatialKNN instance.
            To write out this SpatialKNN instance to persistent storage, use the save method
            on the returned instance.
        """

        return self.model.write

    @staticmethod
    def read(self):
        """
        return: a SpatialKNNReader instance for this SpatialKNN instance.
            To load this SpatialKNN instance from persistent storage, use the load method
            on the returned instance.
        """

        return self.model.read

    def load(self, path):
        """

        """

        return self.model.load(path)