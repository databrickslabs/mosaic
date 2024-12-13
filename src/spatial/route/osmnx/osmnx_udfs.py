__all__ = [
    "osmnx_nearest_node_udf",
    "osmnx_route_distance_weighted_udf",
    "osmnx_route_time_weighted_udf"
]
from .osmnx_utils import OsmnxMgr
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import *

import pandas as pd

@pandas_udf(returnType=OsmnxMgr.nearest_node_schema)
def osmnx_nearest_node_udf(lng: pd.Series, lat: pd.Series, graph_pickle_path: pd.Series) -> pd.DataFrame:
    """
    Get nearest graph node from lng/lat coords.
    - loads / uses graphml file for the route in a "pseudo" grouped manner (maintains original index order);
      expects graph to be pickled
    Returns Struct[<nn>,<nn_meters>] for distance of coords to nearest node.
    """
    return OsmnxMgr.nearest_node(lng, lat, graph_pickle_path, pickled_graph=True)

@pandas_udf(returnType=StringType())
def osmnx_route_distance_weighted_udf(
        start_node: pd.Series, end_node: pd.Series, graph_pickle_path: pd.Series
) -> pd.Series:
    """
    Generate routes from start/end graph nodes.
    - start_node, end_node, graph_path are all Series (None and NaN values are identified and handled)
    - loads / uses graphml file for the route in a "pseudo" grouped manner (maintains original index order);
      expects graph to be pickled
    - weight used is 'length'
    - uses 1 cpu (same as spark defaults)
    Returns each route as a json string (list of dicts) which can be parsed out.
    """
    return OsmnxMgr.route(start_node, end_node, graph_pickle_path, weight='length', as_string=True, pickled_graph=True)

@pandas_udf(returnType=StringType())
def osmnx_route_time_weighted_udf(
        start_node: pd.Series, end_node: pd.Series, graph_pickle_path: pd.Series
) -> pd.Series:
    """
    Generate routes from start/end graph nodes.
    - start_node, end_node, graph_path are all Series (None and NaN values are identified and handled)
    - loads / uses graphml file for the route in a "pseudo" grouped manner (maintains original index order);
      expects graph to be pickled
    - weight used is 'travel_time' - must be available in the graph(s)
    - uses 1 cpu (same as spark defaults)
    Returns each route as a json string (list of dicts) which can be parsed out.
    """
    return OsmnxMgr.route(start_node, end_node, graph_pickle_path, weight='travel_time', as_string=True, pickled_graph=True)