from IPython import InteractiveShell
from pyspark.sql import SparkSession, SQLContext

from mosaic.core.mosaic_context import MosaicContext
from mosaic.utils.display_handler import DisplayHandler

mosaic_spark: SparkSession
mosaic_context: MosaicContext
sql_context: SQLContext
display_handler: DisplayHandler
ipython_hook: InteractiveShell
notebook_utils = None
