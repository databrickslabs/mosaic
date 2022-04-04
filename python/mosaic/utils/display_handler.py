import py4j.java_gateway
from pyspark.sql import DataFrame, SparkSession

from mosaic.config import config
import IPython.display as ipydisplay


class DisplayHandler:
    MosaicFrameClass: py4j.java_gateway.JavaClass
    MosaicFrameObject: py4j.java_gateway.JavaObject
    ScalaOptionClass: py4j.java_gateway.JavaClass
    ScalaOptionObject: py4j.java_gateway.JavaObject
    in_databricks: bool
    dataframe_display_function = None

    def __init__(self, spark: SparkSession):
        try:
            from PythonShellImpl import PythonShell

            shell_instance = PythonShell.display.__self__
            self.dataframe_display_function = shell_instance.display
            self.html_display_function = shell_instance.displayHTML
            self.in_databricks = True
        except ImportError:
            self.dataframe_display_function = DataFrame.show  # self.basic_display
            self.html_display_function = self.fallback_display_html
            self.in_databricks = False
        sc = spark.sparkContext
        self.ScalaOptionClass = getattr(sc._jvm.scala, "Option$")
        self.ScalaOptionObject = getattr(self.ScalaOptionClass, "MODULE$")
        self.PrettifierModule = getattr(
            sc._jvm.com.databricks.labs.mosaic.sql, "Prettifier"
        )

    #
    # @staticmethod
    # def basic_display(df: DataFrame):
    #     df.show()

    @staticmethod
    def fallback_display_html(html: str):
        ipydisplay.display(ipydisplay.HTML(html))

    def display_dataframe(self, df: DataFrame):
        prettifier = self.PrettifierModule
        pretty_jdf = (
            prettifier.prettified(df._jdf, self.ScalaOptionObject.apply(None))
            if not type(df) == "MosaicFrame"
            else prettifier.prettifiedMosaicFrame(df._mosaicFrame)
        )
        pretty_df = DataFrame(pretty_jdf, config.sql_context)
        self.dataframe_display_function(pretty_df)

    def display_html(self, html: str):
        self.html_display_function(html)


def displayMosaic(df: DataFrame):
    if not hasattr(config, "display_handler"):
        config.display_handler = DisplayHandler(config.mosaic_spark)
    config.display_handler.display_dataframe(df)
#
#
# def displayHTML(html: str):
#     if not hasattr(config, "display_handler"):
#         config.display_handler = DisplayHandler(config.mosaic_spark)
#     config.display_handler.display_html(html)
