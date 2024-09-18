from pyspark.sql import SparkSession

__all__ = [
    "get_fun_prefix",
    "unregister"
]

DEFAULT_PREFIX = "dbx"

def get_fun_prefix(override_prefix: str = ""):
    """
    Get prefix to use in function, defaults to "dbx_".
    - returns override_prefix as-is if it contains '_'
    - otherwise, returns with '_' appended

    :param override_prefix: optional, specify prefix
    :return: "<prefix>_"
    """
    if override_prefix:
        if override_prefix.endswith('_'):
            return override_prefix
        else:
            return f"{override_prefix}_"
    else:
        return f"{DEFAULT_PREFIX}_"


def unregister(fun_name: str, spark: SparkSession, catalog: str, schema: str, override_prefix: str = ""):
    """
    Un-register a function.

    :param fun_name: name of function (minus prefix)
    :param spark: spark session to use
    :param catalog: catalog name
    :param schema: schema name
    :param override_prefix: use value if specified
    """
    fun_prefix = get_fun_prefix(override_prefix=override_prefix)
    fun = f"{fun_prefix}{fun_name}"
    spark.sql(f"""DROP FUNCTION IF EXISTS {catalog}.{schema}.{fun}""")