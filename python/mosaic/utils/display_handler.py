import py4j.java_gateway
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import _to_java_column as pyspark_to_java_column

from mosaic.config import config
from mosaic.utils.types import ColumnOrName


class DisplayHandler:

    MosaicFrameClass: py4j.java_gateway.JavaClass
    MosaicFrameObject: py4j.java_gateway.JavaObject
    ScalaOptionClass: py4j.java_gateway.JavaClass
    ScalaOptionObject: py4j.java_gateway.JavaObject
    in_databricks: bool
    display_function = None

    def __init__(self, spark: SparkSession):
        try:
            from PythonShellImpl import PythonShell

            self.display_function = PythonShell.display
            self.in_databricks = True
        except ImportError:
            self.display_function = self.basic_display
            self.in_databricks = False
        sc = spark.sparkContext
        self.ScalaOptionClass = getattr(sc._jvm.scala, "Option$")
        self.ScalaOptionObject = getattr(self.ScalaOptionClass, "MODULE$")
        self.MosaicFrameClass = getattr(
            sc._jvm.com.databricks.mosaic.sql, "MosaicFrame$"
        )
        self.MosaicFrameCompanionObject = getattr(self.MosaicFrameClass, "MODULE$")

    def make_option(
        self, col_name: ColumnOrName = None
    ) -> py4j.java_gateway.JavaObject:
        if col_name:
            return self.ScalaOptionObject.apply(pyspark_to_java_column(col_name))
        return self.ScalaOptionObject.apply(None)

    @staticmethod
    def basic_display(df: DataFrame):
        df.show()

    def display(
        self,
        df: DataFrame,
        chip_column: ColumnOrName,
        chip_flag_column: ColumnOrName,
        index_column: ColumnOrName,
        geometry_column: ColumnOrName,
    ):
        mosaic_jdf = self.MosaicFrameCompanionObject.apply(
            df._jdf,
            self.make_option(chip_column),
            self.make_option(chip_flag_column),
            self.make_option(index_column),
            self.make_option(geometry_column),
        ).prettified()
        pretty_df = DataFrame(mosaic_jdf, config.sql_context)
        self.display_function(pretty_df)


def displayMosaic(
    df: DataFrame,
    chip_column: ColumnOrName = None,
    chip_flag_column: ColumnOrName = None,
    index_column: ColumnOrName = None,
    geometry_column: ColumnOrName = None,
):
    if not hasattr(config, "display_handler"):
        config.display_handler = DisplayHandler(config.mosaic_spark)
    config.display_handler.display(
        df, chip_column, chip_flag_column, index_column, geometry_column
    )
