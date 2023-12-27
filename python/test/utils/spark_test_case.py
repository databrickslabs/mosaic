from importlib.metadata import version
from pyspark.sql import SparkSession

import mosaic
import os
import unittest

class SparkTestCase(unittest.TestCase):
    spark = None
    library_location = None
    
    @classmethod
    def setUpClass(cls) -> None:
        cls.library_location = f"{mosaic.__path__[0]}/lib/mosaic-{version('databricks-mosaic')}-jar-with-dependencies.jar"
        if not os.path.exists(cls.library_location):
            cls.library_location = f"{mosaic.__path__[0]}/lib/mosaic-{version('databricks-mosaic')}-SNAPSHOT-jar-with-dependencies.jar"


        cls.spark = (
            SparkSession.builder.master("local")
            .config("spark.jars", cls.library_location)
            .getOrCreate()
        )
        cls.spark.conf.set("spark.databricks.labs.mosaic.jar.autoattach", "false")
    
    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def setUp(self) -> None:
        log4j = self.spark.sparkContext._jvm.org.apache.log4j
        log4j.LogManager.getLogger("log4j.logger.org.apache.spark.api.python.PythonGatewayServer").setLevel(log4j.Level.FATAL)
        return super().setUp()