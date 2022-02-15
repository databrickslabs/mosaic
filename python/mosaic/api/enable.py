from pyspark.sql import SparkSession

from mosaic.config import config
from mosaic.core.library_handler import MosaicLibraryHandler
from mosaic.core.mosaic_context import MosaicContext
from mosaic.utils.display_handler import DisplayHandler


def enable_mosaic(spark: SparkSession) -> None:
    """
    Enable Mosaic functions.

    Use this function at the start of your workflow to ensure all of the required dependencies are installed and
    Mosaic is configured according to your needs.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
            The active SparkSession.

    Returns
    -------

    Notes
    -----
    Users can control various aspects of Mosaic's operation with the following Spark confs:

    - `spark.databricks.mosaic.jar.autoattach`: 'true' (default) or 'false'
       Automatically attach the Mosaic JAR to the Databricks cluster? (Optional)
    - `spark.databricks.mosaic.jar.location`
       Explicitly specify the path to the Mosaic JAR.
       (Optional and not required at all in a standard Databricks environment).
    - `spark.databricks.mosaic.geometry.api`: 'OGC' (default) or 'JTS'
       Explicitly specify the underlying geometry library to use for spatial operations. (Optional)
    - `spark.databricks.mosaic.index.system`: 'H3' (default)
       Explicitly specify the index system to use for optimized spatial joins. (Optional)

    """
    config.mosaic_spark = spark
    _ = MosaicLibraryHandler(config.mosaic_spark)
    config.mosaic_context = MosaicContext(config.mosaic_spark)
