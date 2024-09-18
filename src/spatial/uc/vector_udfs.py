from pyspark.sql import SparkSession
from spatial.uc.udf_helpers import get_fun_prefix, unregister

__all__ = [
  "register_all",
  "unregister_all",
  "register_explain_validity",
  "register_flatten_polygons",
  "register_is_poly_node",
  "register_make_valid",
  "register_transform"
]

udf_names = [
  "explain_validity",
  "flatten_polygons",
  "is_poly_node",
  "make_valid",
  "transform"
]

def register_all(spark: SparkSession, catalog: str, schema: str, override_prefix: str = ""):
  """
  Register vector functions with unity catalog.
  - un-registers all existing (under the prefix) prior.
  - this helps with clarity in case there is any issues.

  :param spark: spark session to use
  :param catalog: catalog name
  :param schema: schema name
  :param override_prefix: use value if specified
  """
  # - unregister all
  unregister_all(spark, catalog, schema, override_prefix=override_prefix)

  # - register all
  register_explain_validity(spark, catalog, schema, override_prefix=override_prefix)
  register_flatten_polygons(spark, catalog, schema, override_prefix=override_prefix)
  register_is_poly_node(spark, catalog, schema, override_prefix=override_prefix)
  register_make_valid(spark, catalog, schema, override_prefix=override_prefix)
  register_transform(spark, catalog, schema, override_prefix=override_prefix)

def register_explain_validity(spark: SparkSession, catalog: str, schema: str, override_prefix: str = ""):
  """
  Register function.
  - un-registers (under the prefix) prior.

  :param spark: spark session to use
  :param catalog: catalog name
  :param schema: schema name
  :param override_prefix: use value if specified
  """
  fun_name = "explain_validity"
  unregister(fun_name, spark, catalog, schema, override_prefix=override_prefix)
  fun_prefix = get_fun_prefix(override_prefix=override_prefix)
  spark.sql(
f"""CREATE FUNCTION {catalog}.{schema}.{fun_prefix}{fun_name}(wkt STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Explain the (in)validity of a geometry.'
AS $$
  from shapely.wkt import loads
  from spatial.vector import vector_utils
  try:
    return vector_utils.explain_validity(loads(wkt))
  except Exception as e:
    return ""
$$"""
  )

def register_flatten_polygons(spark: SparkSession, catalog: str, schema: str, override_prefix: str = ""):
  """
  Register function.
  - un-registers (under the prefix) prior.

  :param spark: spark session to use
  :param catalog: catalog name
  :param schema: schema name
  :param override_prefix: use value if specified
  """
  fun_name = "flatten_polygons"
  unregister(fun_name, spark, catalog, schema, override_prefix=override_prefix)
  fun_prefix = get_fun_prefix(override_prefix=override_prefix)
  spark.sql(
f"""CREATE FUNCTION {catalog}.{schema}.{fun_prefix}{fun_name}(wkt STRING)
RETURNS ARRAY<STRING>
LANGUAGE PYTHON
AS $$
  from shapely.wkt import loads
  from spatial.vector import vector_utils
  try: 
    return [p.wkt for p in vector_utils.flatten_polygons(loads(wkt))]
  except Exception as e:
    return [wkt]
$$"""
  )

def register_is_poly_node(spark: SparkSession, catalog: str, schema: str, override_prefix: str = ""):
  """
  Register function.
  - un-registers (under the prefix) prior.

  :param spark: spark session to use
  :param catalog: catalog name
  :param schema: schema name
  :param override_prefix: use value if specified
  """
  fun_name = "is_poly_node"
  unregister(fun_name, spark, catalog, schema, override_prefix=override_prefix)
  fun_prefix = get_fun_prefix(override_prefix=override_prefix)
  spark.sql(
f"""CREATE FUNCTION {catalog}.{schema}.{fun_prefix}{fun_name}(wkt STRING, x DOUBLE, y DOUBLE)
RETURNS BOOLEAN
LANGUAGE PYTHON
AS $$
  from shapely.wkt import loads
  from spatial.vector import vector_utils
  try:
    return vector_utils.is_poly_node(loads(wkt), x, y)
  except Exception as e:
    return False
$$"""
  )

def register_make_valid(spark: SparkSession, catalog: str, schema: str, override_prefix: str = ""):
  """
  Register function.
  - un-registers (under the prefix) prior.

  :param spark: spark session to use
  :param catalog: catalog name
  :param schema: schema name
  :param override_prefix: use value if specified
  """
  fun_name = "make_valid"
  unregister(fun_name, spark, catalog, schema, override_prefix=override_prefix)
  fun_prefix = get_fun_prefix(override_prefix=override_prefix)
  spark.sql(
f"""CREATE FUNCTION {catalog}.{schema}.{fun_prefix}{fun_name}(wkt STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
  from shapely.wkt import loads
  from spatial.vector import vector_utils
  try:
    return vector_utils.make_valid(loads(wkt)).wkt
  except Exception as e:
    return None
$$"""
  )

def register_transform(spark: SparkSession, catalog: str, schema: str, override_prefix: str = ""):
  """
   Register function.
   - un-registers (under the prefix) prior.

   :param spark: spark session to use
   :param catalog: catalog name
   :param schema: schema name
   :param override_prefix: use value if specified
   """
  fun_name = "transform"
  unregister(fun_name, spark, catalog, schema, override_prefix=override_prefix)
  fun_prefix = get_fun_prefix(override_prefix=override_prefix)
  spark.sql(
f"""CREATE FUNCTION {catalog}.{schema}.{fun_prefix}{fun_name}(wkt STRING, from_crs STRING, to_crs STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
  from shapely.wkt import loads
  from spatial.vector import vector_utils
  try:
    return vector_utils.transform(loads(wkt), from_crs, to_crs)
  except Exception as e:
    return None
$$"""
  )

def unregister_all(spark: SparkSession, catalog: str, schema: str, override_prefix: str = ""):
  """
  Un-register vector functions with unity catalog.

  :param spark: spark session to use to un-register
  :param catalog: catalog name
  :param schema: schema name
  :param override_prefix: use value if specified
  """
  for f in udf_names:
    unregister(f, spark, catalog, schema, override_prefix=override_prefix)
