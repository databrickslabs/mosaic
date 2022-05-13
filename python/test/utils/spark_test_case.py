import unittest
from importlib.metadata import version

from pyspark.sql import SparkSession

import mosaic


class SparkTestCase(unittest.TestCase):

    spark = None
    library_location = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.library_location = f"{mosaic.__path__[0]}/lib/mosaic-{version('databricks-mosaic')}-jar-with-dependencies.jar"
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .config("spark.jars", cls.library_location)
            .getOrCreate()
        )
        cls.spark.conf.set("spark.databricks.labs.mosaic.jar.autoattach", "false")
        cls.spark.sparkContext.setLogLevel("warn")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.sparkContext.setLogLevel("warn")
        cls.spark.stop()
