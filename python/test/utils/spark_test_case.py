import os
import shutil
import unittest
import warnings
from importlib.metadata import version

from pyspark.sql import SparkSession

import mosaic


class SparkTestCase(unittest.TestCase):
    spark = None
    library_location = None
    log4jref = None
    tmp_dir = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.library_location = f"{mosaic.__path__[0]}/lib/mosaic-{version('databricks-mosaic')}-jar-with-dependencies.jar"
        if not os.path.exists(cls.library_location):
            cls.library_location = f"{mosaic.__path__[0]}/lib/mosaic-{version('databricks-mosaic')}-SNAPSHOT-jar-with-dependencies.jar"

        pwd_dir = os.getcwd()
        cls.tmp_dir = f"{pwd_dir}/mosaic_test/"
        if not os.path.exists(cls.tmp_dir):
            os.makedirs(cls.tmp_dir)

        cls.spark = (
            SparkSession.builder.master("local[*]")
            .config("spark.jars", cls.library_location)
            .config("spark.driver.memory", "4g")
            .config(
                "spark.driver.extraJavaOptions",
                "-Dorg.apache.logging.log4j.level=ERROR",
            )
            .config(
                "spark.executor.extraJavaOptions",
                "-Dorg.apache.logging.log4j.level=ERROR",
            )
            .getOrCreate()
        )
        cls.spark.conf.set("spark.databricks.labs.mosaic.test.mode", "true")
        cls.spark.conf.set("spark.databricks.labs.mosaic.jar.autoattach", "false")
        cls.spark.conf.set(
            "spark.databricks.labs.mosaic.raster.tmp.prefix", cls.tmp_dir
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
        if cls.tmp_dir is not None and os.path.exists(cls.tmp_dir):
            shutil.rmtree(cls.tmp_dir)

    def setUp(self) -> None:
        self.spark.sparkContext.setLogLevel("ERROR")
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)
