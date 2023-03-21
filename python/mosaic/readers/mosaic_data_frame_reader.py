from pyspark.sql import SparkSession, DataFrame, SQLContext


class MosaicDataFrameReader:
    """
    MosaicDataFrameReader is a wrapper around the MosaicDataFrameReader that allows for offset reading
    of OGR data sources. This provides a means to read data sources that are too large to fit into memory
    in a single read. This is accomplished by reading the data source in chunks and using the generators.
    """

    def __init__(self):
        """
        Initialize the MosaicDataFrameReader.
        """
        self.spark = SparkSession.builder.getOrCreate()
        self.reader = getattr(
            self.spark._jvm.com.databricks.labs.mosaic.datasource.multiread, "MosaicDataFrameReader"
        )(self.spark._jsparkSession)

    def format(self, format):
        """
        Set the format of the data source to read.
        :param format: Format of the data source to read.
        :return: MosaicDataFrameReader with the param set to provided value.
        """
        self.reader.format(format)
        return self

    def option(self, key, value):
        """
        Set the option for the data source to read.
        :param key: Option key.
        :param value: Option value.
        :return: MosaicDataFrameReader with the param set to provided value.
        """
        self.reader.option(key, value)
        return self

    def load(self, path):
        """
        Load the data source as a MosaicDataFrame.
        :param path: Path to the data source.
        :return: MosaicDataFrame.
        """
        df = self.reader.load(path)
        return DataFrame(df, SQLContext(self.spark.sparkContext))

    def load(self, *paths):
        """
        Load the data source as a MosaicDataFrame.
        :param paths: Paths to the data source.
        :return: MosaicDataFrame.
        """
        df = self.reader.load(*paths)
        return DataFrame(df, SQLContext(self.spark.sparkContext))
