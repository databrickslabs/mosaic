import warnings

from IPython.core.getipython import get_ipython
from pyspark import SQLContext
from pyspark.sql import SparkSession

from mosaic.config import config
from mosaic.core.library_handler import MosaicLibraryHandler
from mosaic.core.mosaic_context import MosaicContext
from mosaic.utils.notebook_utils import NotebookUtils


def enable_mosaic(
    spark: SparkSession,
    dbutils=None,
    log_info: bool = False,
    jar_path: str = None,
    jar_autoattach: bool = True,
) -> None:
    """
    Enable Mosaic functions.

    Use this function at the start of your workflow to ensure all the required dependencies are installed and
    Mosaic is configured according to your needs.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
            The active SparkSession.
    dbutils : dbruntime.dbutils.DBUtils
            Optional, specify dbutils object used for `display` and `displayHTML` functions.
    log_info : bool
            Logging cannot be adjusted with Unity Catalog Shared Access clusters;
            if you try to do so, will throw a Py4JSecurityException.
             - True will try to setLogLevel to 'info'
             - False will not; Default is False
    jar_path : str
            Convenience when you need to change the JAR path for Unity Catalog
            Volumes with Shared Access clusters
              - Default is None; if provided, sets
                "spark.databricks.labs.mosaic.jar.path"
    jar_autoattach : bool
            Convenience when you need to turn off JAR auto-attach for Unity
            Catalog Volumes with Shared Access clusters.
              - False will not registers the JAR; sets
                "spark.databricks.labs.mosaic.jar.autoattach" to "false"
              - True will register the JAR; Default is True


    Returns
    -------

    Notes
    -----
    Users can control various aspects of Mosaic's operation with the following Spark confs:

    - `spark.databricks.labs.mosaic.jar.autoattach`: 'true' (default) or 'false'
       Automatically attach the Mosaic JAR to the Databricks cluster? (Optional)
    - `spark.databricks.labs.mosaic.jar.path`
       Explicitly specify the path to the Mosaic JAR.
       (Optional and not required at all in a standard Databricks environment).
    - `spark.databricks.labs.mosaic.geometry.api`: 'JTS'
       Explicitly specify the underlying geometry library to use for spatial operations. (Optional)
    - `spark.databricks.labs.mosaic.index.system`: 'H3' (default)
       Explicitly specify the index system to use for optimized spatial joins. (Optional)

    """
    # Set spark session, conditionally:
    # - set conf for jar autoattach
    # - set conf for jar path
    # - set log level to 'info'
    if not jar_autoattach:
        spark.conf.set("spark.databricks.labs.mosaic.jar.autoattach", "false")
        print("...set 'spark.databricks.labs.mosaic.jar.autoattach' to false")
    if jar_path is not None:
        spark.conf.set("spark.databricks.labs.mosaic.jar.path", jar_path)
        print(f"...set 'spark.databricks.labs.mosaic.jar.path' to '{jar_path}'")
    if log_info:
        spark.sparkContext.setLogLevel("info")
    config.mosaic_spark = spark
    _ = MosaicLibraryHandler(config.mosaic_spark, log_info=log_info)
    config.mosaic_context = MosaicContext(config.mosaic_spark)

    # Register SQL functions
    optionClass = getattr(spark._sc._jvm.scala, "Option$")
    optionModule = getattr(optionClass, "MODULE$")
    config.mosaic_context._context.register(
        spark._jsparkSession, optionModule.apply(None)
    )

    isSupported = config.mosaic_context._context.checkDBR(spark._jsparkSession)
    if not isSupported:
        # unexpected - checkDBR returns true or throws exception
        print("""WARNING: checkDBR returned False.""")

    # Not yet added to the pyspark API
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        config.sql_context = SQLContext(spark.sparkContext)

    config.notebook_utils = dbutils.notebook if dbutils else NotebookUtils
    config.ipython_hook = get_ipython()
    if config.ipython_hook:
        from mosaic.utils.kepler_magic import MosaicKepler

        config.ipython_hook.register_magics(MosaicKepler)
