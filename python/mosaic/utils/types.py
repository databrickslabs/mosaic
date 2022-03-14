from typing import Union, Any
from mosaic.config import config

from pyspark.sql import Column
from pyspark.sql.column import _to_java_column

ColumnOrName = Union[Column, str]


def scala_option(value: Any):
    return config.mosaic_spark.sparkContext._jvm.scala.Some(value)


def column_option(value: Any):
    scala_option(_to_java_column(value)) if value else scala_option(None)
