import unittest

from pyspark.sql import SparkSession


class SparkTestCase(unittest.TestCase):

    spark = None
    library_location = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.library_location = (
            "mosaic/lib/mosaic-1.0-SNAPSHOT-jar-with-dependencies.jar"
        )
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .config("spark.jars", cls.library_location)
            .getOrCreate()
        )
        cls.spark.conf.set("spark.databricks.mosaic.jar.autoattach", "false")
        cls.spark.conf.set("spark.databricks.mosaic.jar.path", cls.library_location)
        cls.spark.sparkContext.setLogLevel("warn")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.sparkContext.setLogLevel("warn")
        cls.spark.stop()
