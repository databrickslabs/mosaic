import warnings

from IPython.core.getipython import get_ipython
from pyspark import SQLContext
from pyspark.sql import SparkSession

from mosaic.config import config
from mosaic.core.library_handler import MosaicLibraryHandler
from mosaic.core.mosaic_context import MosaicContext
from mosaic.utils.notebook_utils import NotebookUtils


def enable_mosaic(spark: SparkSession, dbutils=None) -> None:
    """
    Enable Mosaic functions.

    Use this function at the start of your workflow to ensure all the required dependencies are installed and
    Mosaic is configured according to your needs.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
            The active SparkSession.
    dbutils : dbruntime.dbutils.DBUtils
            The dbutils object used for `display` and `displayHTML` functions.
            Optional, only applicable to Databricks users.

    Returns
    -------

    Notes
    -----
    Users can control various aspects of Mosaic's operation with the following Spark confs:

    - `spark.databricks.labs.mosaic.jar.autoattach`: 'true' (default) or 'false'
       Automatically attach the Mosaic JAR to the Databricks cluster? (Optional)
    - `spark.databricks.labs.mosaic.jar.location`
       Explicitly specify the path to the Mosaic JAR.
       (Optional and not required at all in a standard Databricks environment).
    - `spark.databricks.labs.mosaic.geometry.api`: 'ESRI' (default) or 'JTS'
       Explicitly specify the underlying geometry library to use for spatial operations. (Optional)
    - `spark.databricks.labs.mosaic.index.system`: 'H3' (default)
       Explicitly specify the index system to use for optimized spatial joins. (Optional)

    """
    config.mosaic_spark = spark
    _ = MosaicLibraryHandler(config.mosaic_spark)
    config.mosaic_context = MosaicContext(config.mosaic_spark)

    # Register SQL functions
    optionClass = getattr(spark._sc._jvm.scala, "Option$")
    optionModule = getattr(optionClass, "MODULE$")
    config.mosaic_context._context.register(
        spark._jsparkSession, optionModule.apply(None)
    )

    # Not yet added to the pyspark API
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        config.sql_context = SQLContext(spark.sparkContext)

    config.notebook_utils = dbutils.notebook if dbutils else NotebookUtils
    config.ipython_hook = get_ipython()
    if config.ipython_hook:
        from mosaic.utils.kepler_magic import MosaicKepler

        config.ipython_hook.register_magics(MosaicKepler)
