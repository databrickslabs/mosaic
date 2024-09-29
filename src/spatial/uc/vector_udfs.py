from pyspark.sql import SparkSession
from .udf_helpers import get_fun_prefix, unregister

__all__ = [
  "register_all",
  "unregister_all",
  "register_buffer_cap_style",
  "register_buffer_loop",
  "register_concave_hull",
  "register_explain_validity",
  "register_has_node_startswith",
  "register_has_valid_coordinates",
  "register_make_valid",
  "register_multi_flatten",
  "register_point_z",
  "register_test_valid_coordinates",
  "register_unary_union",
  "register_unary_union_array",
  "register_update_srid"
]

udf_names = [
  "buffer_cap_style",
  "buffer_loop",
  "concave_hull",
  "explain_validity",
  "has_node_startswith",
  "has_valid_coordinates",
  "make_valid",
  "multi_flatten",
  "point_z",
  "test_valid_coordinates",
  "unary_union",
  "unary_union_array",
  "update_srid"
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
  register_buffer_cap_style(spark, catalog, schema, override_prefix=override_prefix)
  register_buffer_loop(spark, catalog, schema, override_prefix=override_prefix)
  register_concave_hull(spark, catalog, schema, override_prefix=override_prefix)
  register_explain_validity(spark, catalog, schema, override_prefix=override_prefix)
  register_has_node_startswith(spark, catalog, schema, override_prefix=override_prefix)
  register_has_valid_coordinates(spark, catalog, schema, override_prefix=override_prefix)
  register_make_valid(spark, catalog, schema, override_prefix=override_prefix)
  register_multi_flatten(spark, catalog, schema, override_prefix=override_prefix)
  register_point_z(spark, catalog, schema, override_prefix=override_prefix)
  register_test_valid_coordinates(spark, catalog, schema, override_prefix=override_prefix)
  register_unary_union(spark, catalog, schema, override_prefix=override_prefix)
  register_unary_union_array(spark, catalog, schema, override_prefix=override_prefix)
  register_update_srid(spark, catalog, schema, override_prefix=override_prefix)


def register_buffer_cap_style(spark: SparkSession, catalog: str, schema: str, override_prefix: str = ""):
  """
  Register function.
  - un-registers (under the prefix) prior.

  :param spark: spark session to use
  :param catalog: catalog name
  :param schema: schema name
  :param override_prefix: use value if specified
  """
  fun_name = "buffer_cap_style"
  unregister(fun_name, spark, catalog, schema, override_prefix=override_prefix)
  fun_prefix = get_fun_prefix(override_prefix=override_prefix)
  spark.sql(
    f"""CREATE FUNCTION {catalog}.{schema}.{fun_prefix}{fun_name}(geom BINARY, radius DOUBLE, cap_style STRING)
RETURNS BINARY
LANGUAGE PYTHON
COMMENT 'Attempt to buffer a geometry and specify the cap_style (round, square, flat); returns ewkb.'
AS $$
  from spatial.vector import vector_utils

  result = vector_utils.try_buffer(geom, radius, cap_style=cap_style)
  return vector_utils.try_to_ewkb(result)
$$"""
  )

def register_buffer_loop(spark: SparkSession, catalog: str, schema: str, override_prefix: str = ""):
  """
  Register function.
  - un-registers (under the prefix) prior.

  :param spark: spark session to use
  :param catalog: catalog name
  :param schema: schema name
  :param override_prefix: use value if specified
  """
  fun_name = "buffer_loop"
  unregister(fun_name, spark, catalog, schema, override_prefix=override_prefix)
  fun_prefix = get_fun_prefix(override_prefix=override_prefix)
  spark.sql(
    f"""CREATE FUNCTION {catalog}.{schema}.{fun_prefix}{fun_name}(geom BINARY, inner_radius DOUBLE, outer_radius DOUBLE, cap_style STRING)
RETURNS BINARY
LANGUAGE PYTHON
COMMENT 'Attempt create a buffer loop a geometry and specify the cap_style (round, square, flat); returns ewkb.'
AS $$
  from spatial.vector import vector_utils

  result = vector_utils.try_buffer_loop(geom, inner_radius, outer_radius, cap_style=cap_style)
  return vector_utils.try_to_ewkb(result)
$$"""
  )

def register_concave_hull(spark: SparkSession, catalog: str, schema: str, override_prefix: str = ""):
  """
  Register function.
  - un-registers (under the prefix) prior.

  :param spark: spark session to use
  :param catalog: catalog name
  :param schema: schema name
  :param override_prefix: use value if specified
  """
  fun_name = "concave_hull"
  unregister(fun_name, spark, catalog, schema, override_prefix=override_prefix)
  fun_prefix = get_fun_prefix(override_prefix=override_prefix)
  spark.sql(
    f"""CREATE FUNCTION {catalog}.{schema}.{fun_prefix}{fun_name}(geom BINARY, ratio DOUBLE, allow_holes BOOLEAN)
RETURNS BINARY
LANGUAGE PYTHON
COMMENT 'Computes a concave geometry that encloses an input geometry; returns ewkb.'
AS $$
  from spatial.vector import vector_utils

  result = vector_utils.try_concave_hull(geom, ratio, allow_holes=allow_holes)
  return vector_utils.try_to_ewkb(result)
$$"""
  )

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
f"""CREATE FUNCTION {catalog}.{schema}.{fun_prefix}{fun_name}(geom BINARY)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Explain the (in)validity of a geometry; accepts various interchange formats and returns explanation.'
AS $$
  from spatial.vector import vector_utils
  
  result = vector_utils.try_explain_validity(geom)
  if result:
    return result
  else:
    return "explain_validity -> geom not parsed"
$$"""
  )

def register_has_node_startswith(spark: SparkSession, catalog: str, schema: str, override_prefix: str = ""):
  """
  Register function.
  - un-registers (under the prefix) prior.

  :param spark: spark session to use
  :param catalog: catalog name
  :param schema: schema name
  :param override_prefix: use value if specified
  """
  fun_name = "has_node_startswith"
  unregister(fun_name, spark, catalog, schema, override_prefix=override_prefix)
  fun_prefix = get_fun_prefix(override_prefix=override_prefix)
  spark.sql(
f"""CREATE FUNCTION {catalog}.{schema}.{fun_prefix}{fun_name}(geom BINARY, x DOUBLE, y DOUBLE)
RETURNS BOOLEAN
LANGUAGE PYTHON
COMMENT 'Look for geoms with nodes starting with xy values; accepts various interchange formats and returns True | False.'
AS $$
  from spatial.vector import vector_utils
  
  return vector_utils.has_node_startswith(geom, x, y)
$$"""
  )

def register_has_valid_coordinates(spark: SparkSession, catalog: str, schema: str, override_prefix: str = ""):
  """
  Register function.
  - un-registers (under the prefix) prior.

  :param spark: spark session to use
  :param catalog: catalog name
  :param schema: schema name
  :param override_prefix: use value if specified
  """
  fun_name = "has_valid_coordinates"
  unregister(fun_name, spark, catalog, schema, override_prefix=override_prefix)
  fun_prefix = get_fun_prefix(override_prefix=override_prefix)
  spark.sql(
    f"""CREATE FUNCTION {catalog}.{schema}.{fun_prefix}{fun_name}(geom BINARY)
RETURNS BOOLEAN
LANGUAGE PYTHON
COMMENT 'Checks if all points in geom are valid with respect its existing crs bounds.'
AS $$
  from spatial.vector import vector_utils

  return vector_utils.has_valid_coordinates(geom)
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
f"""CREATE FUNCTION {catalog}.{schema}.{fun_prefix}{fun_name}(geom BINARY)
RETURNS BINARY
LANGUAGE PYTHON
COMMENT 'Attempt to make invalid geometries valid; accepts various interchange formats and returns ewkb.'
AS $$
  from spatial.vector import vector_utils
  
  result = vector_utils.try_make_valid(geom)
  return vector_utils.try_to_ewkb(result)
$$"""
  )

def register_multi_flatten(spark: SparkSession, catalog: str, schema: str, override_prefix: str = ""):
  """
  Register function.
  - un-registers (under the prefix) prior.

  :param spark: spark session to use
  :param catalog: catalog name
  :param schema: schema name
  :param override_prefix: use value if specified
  """
  fun_name = "multi_flatten"
  unregister(fun_name, spark, catalog, schema, override_prefix=override_prefix)
  fun_prefix = get_fun_prefix(override_prefix=override_prefix)
  spark.sql(
f"""CREATE FUNCTION {catalog}.{schema}.{fun_prefix}{fun_name}(geom BINARY)
RETURNS ARRAY<BINARY>
LANGUAGE PYTHON
COMMENT 'Flatten multi-* geoms if provided; accepts various interchange formats and returns ewkb array or None.'
AS $$
  from spatial.vector import vector_utils
  
  result = vector_utils.try_multi_flatten(geom)
  if result:
    return [vector_utils.try_to_ewkb(geom) for p in result] 
  result1 = vector_utils.try_to_ewkb(geom)
  if result1:
    return [result1]
  return None    
$$"""
  )

def register_point_z(spark: SparkSession, catalog: str, schema: str, override_prefix: str = ""):
  """
  Register function.
  - un-registers (under the prefix) prior.

  :param spark: spark session to use
  :param catalog: catalog name
  :param schema: schema name
  :param override_prefix: use value if specified
  """
  fun_name = "point_z"
  unregister(fun_name, spark, catalog, schema, override_prefix=override_prefix)
  fun_prefix = get_fun_prefix(override_prefix=override_prefix)
  spark.sql(
    f"""CREATE FUNCTION {catalog}.{schema}.{fun_prefix}{fun_name}(geom BINARY)
RETURNS DOUBLE
LANGUAGE PYTHON
COMMENT 'Try to get the Z value of a point; return NaN if not found.'
AS $$
  from spatial.vector import vector_utils

  return vector_utils.get_point_z(geom)
$$"""
  )

def register_test_valid_coordinates(spark: SparkSession, catalog: str, schema: str, override_prefix: str = ""):
  """
  Register function.
  - un-registers (under the prefix) prior.

  :param spark: spark session to use
  :param catalog: catalog name
  :param schema: schema name
  :param override_prefix: use value if specified
  """
  fun_name = "test_valid_coordinates"
  unregister(fun_name, spark, catalog, schema, override_prefix=override_prefix)
  fun_prefix = get_fun_prefix(override_prefix=override_prefix)
  spark.sql(
    f"""CREATE FUNCTION {catalog}.{schema}.{fun_prefix}{fun_name}(geom BINARY, test_srid INTEGER)
RETURNS BOOLEAN
LANGUAGE PYTHON
COMMENT 'Checks if all points in geom are / would be valid with respect to the provided srid bounds.'
AS $$
  from spatial.vector import vector_utils

  return vector_utils.test_valid_coordinates(geom, test_srid)
$$"""
  )

def register_unary_union(spark: SparkSession, catalog: str, schema: str, override_prefix: str = ""):
  """
  Register function.
  - un-registers (under the prefix) prior.

  :param spark: spark session to use
  :param catalog: catalog name
  :param schema: schema name
  :param override_prefix: use value if specified
  """
  fun_name = "unary_union"
  unregister(fun_name, spark, catalog, schema, override_prefix=override_prefix)
  fun_prefix = get_fun_prefix(override_prefix=override_prefix)
  spark.sql(
    f"""CREATE FUNCTION {catalog}.{schema}.{fun_prefix}{fun_name}(geom BINARY)
RETURNS BINARY
LANGUAGE PYTHON
COMMENT 'Returns a geometry that represents the point set union of the given geometry; returns ewkb.'
AS $$
  from spatial.vector import vector_utils

  result = vector_utils.try_unary_union(geom)
  return vector_utils.try_to_ewkb(result)
$$"""
  )

def register_unary_union_array(spark: SparkSession, catalog: str, schema: str, override_prefix: str = ""):
  """
  Register function.
  - un-registers (under the prefix) prior.

  :param spark: spark session to use
  :param catalog: catalog name
  :param schema: schema name
  :param override_prefix: use value if specified
  """
  fun_name = "unary_union_array"
  unregister(fun_name, spark, catalog, schema, override_prefix=override_prefix)
  fun_prefix = get_fun_prefix(override_prefix=override_prefix)
  spark.sql(
    f"""CREATE FUNCTION {catalog}.{schema}.{fun_prefix}{fun_name}(geoms ARRAY<BINARY>)
RETURNS BINARY
LANGUAGE PYTHON
COMMENT 'Returns a geometry that represents the point set union of the given geometries; returns ewkb.'
AS $$
  from spatial.vector import vector_utils

  result = vector_utils.try_unary_union(geoms)
  return vector_utils.try_to_ewkb(result)
$$"""
  )

def register_update_srid(spark: SparkSession, catalog: str, schema: str, override_prefix: str = ""):
  """
   Register function.
   - un-registers (under the prefix) prior.

   :param spark: spark session to use
   :param catalog: catalog name
   :param schema: schema name
   :param override_prefix: use value if specified
  """
  fun_name = "update_srid"
  unregister(fun_name, spark, catalog, schema, override_prefix=override_prefix)
  fun_prefix = get_fun_prefix(override_prefix=override_prefix)
  spark.sql(
f"""CREATE FUNCTION {catalog}.{schema}.{fun_prefix}{fun_name}(geom BINARY, from_srid INTEGER, to_srid INTEGER)
RETURNS BINARY
LANGUAGE PYTHON
COMMENT 'Transform a geometry by using from/to srids; accepts various interchange formats and returns ewkb with srid set.'
AS $$
  from spatial.vector import vector_utils
  
  result = vector_utils.try_update_srid(geom, from_srid, to_srid)
  return vector_utils.try_to_ewkb(result)
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
