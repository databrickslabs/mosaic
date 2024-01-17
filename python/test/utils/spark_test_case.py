from importlib.metadata import version
from pyspark.sql import SparkSession

import mosaic
import os
import unittest

class SparkTestCase(unittest.TestCase):
    spark = None
    library_location = None
    log4jref = None
    
    @classmethod
    def setUpClass(cls) -> None:
        cls.library_location = f"{mosaic.__path__[0]}/lib/mosaic-{version('databricks-mosaic')}-jar-with-dependencies.jar"
        if not os.path.exists(cls.library_location):
            cls.library_location = f"{mosaic.__path__[0]}/lib/mosaic-{version('databricks-mosaic')}-SNAPSHOT-jar-with-dependencies.jar"
        cls.spark = (
            SparkSession.builder.master("local")
            .config("spark.jars", cls.library_location)
            .config("spark.driver.extraJavaOptions", "-Dorg.apache.logging.log4j.level=FATAL")
            .config("spark.executor.extraJavaOptions", "-Dorg.apache.logging.log4j.level=FATAL")
            .getOrCreate()
        )
        cls.spark.conf.set("spark.databricks.labs.mosaic.jar.autoattach", "false")
        cls.spark.sparkContext.setLogLevel("FATAL")
    
    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def setUp(self) -> None:
        self.spark.sparkContext.setLogLevel("FATAL")
