from typing import Any, Union

from pyspark.sql import Column
from pyspark.sql.column import _to_java_column
from pyspark.sql.functions import col

from mosaic.config import config

ColumnOrName = Union[Column, str]


def as_typed_col(value: ColumnOrName, data_type: str):
    return (
        value.cast(data_type)
        if isinstance(value, Column)
        else col(value).cast(data_type)
    )


def scala_option(value: Any):
    return config.mosaic_spark.sparkContext._jvm.scala.Some(value)


def column_option(value: Any):
    scala_option(_to_java_column(value)) if value else scala_option(None)
