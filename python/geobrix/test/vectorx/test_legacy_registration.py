import logging
import pytest
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()   # simpler, robust
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()
JAR_URI = JAR.as_uri()

@pytest.fixture(scope="session")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    spark = (SparkSession.builder
             .config("spark.driver.extraJavaOptions", "-Dlog4j.rootLogger=INFO,console")
             .getOrCreate())
    spark.addArtifacts(JAR_URI)
    spark.sql("LIST JAR").show(truncate=False)
    return spark


def test_legacy_functions_registration(spark):
    from databricks.labs.gbx.vectorx.jts.legacy import functions as legacy_funcs
    legacy_funcs.register(spark)
    df = spark.sql("show functions like 'gbx_st_legacyaswkb'")
    assert df.count() is not None
