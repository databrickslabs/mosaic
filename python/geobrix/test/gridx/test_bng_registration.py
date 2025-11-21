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


def test_bng_functions_registration(spark):
    from databricks.labs.gbx.gridx.bng import functions as bng_funcs
    bng_funcs.register(spark)
    df = spark.range(1).select(bng_funcs.bng_aswkb(f.lit("TQ388791")).alias("wkb"))
    row = df.collect()[0]
    assert row["wkb"] is not None
