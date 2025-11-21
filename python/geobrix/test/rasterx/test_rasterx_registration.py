import logging
import pytest
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()  # simpler, robust
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()
JAR_URI = JAR.as_uri()


@pytest.fixture(scope="session")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    spark = (SparkSession.builder
             .config("spark.driver.extraJavaOptions", "-Dlog4j.rootLogger=ERROR,console")
             .getOrCreate())
    spark.addArtifacts(JAR_URI)
    print(JAR_URI)
    spark.sql("LIST JAR").show(truncate=False)
    return spark


def test_rasterx_functions_registration(spark):
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    path = (HERE.parents[4] / "src/test/resources/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").resolve()
    df = spark.range(1).select(
        rx.rst_avg(
            rx.rst_fromfile(
                f.lit(str(path)),
                f.lit("GTiff"),
            )
        ).alias("avg")
    )
    row = df.collect()[0]
    assert row["avg"] is not None
