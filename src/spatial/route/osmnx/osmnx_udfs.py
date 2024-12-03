from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import *

import pandas as pd

__all__ = [
    "osmnx_nearest_node",
    "osmnx_route_from_nodes"
]

@pandas_udf(
    returnType=StructType([
        StructField("nn", LongType()),
        StructField("nn_meters", DoubleType())
    ])
)
def osmnx_nearest_node(lng: pd.Series, lat: pd.Series, graph_path: pd.Series) -> pd.DataFrame:
    """
    get nearest node from lat/lng
    - loads graphml file(s) to find nearest node
    """
    import osmnx as ox

    pdf = pd.DataFrame({
        "lng": lng,
        "lat": lat,
        "graph_path": graph_path
    })
    pdf_grouped = pdf.groupby('graph_path')

    # iterate over each group
    # load the graphml file for each group
    nns = None
    nn_dists = None
    for graph_path, pdf_data in pdf_grouped:
        G = ox.load_graphml(filepath=graph_path)
        # result is a tuple of (nearest_node, distance) series
        result = ox.distance.nearest_nodes(G, X=pdf_data['lng'], Y=pdf_data['lat'], return_dist=True)
        if nns is None:
            nns = result[0]
        else:
            nns.append(result[0], ignore_index=True)
        if nn_dists is None:
            nn_dists = result[1]
        else:
            nn_dists.append(result[1], ignore_index=True)

    return pd.DataFrame({
        "nn": nns,
        "nn_meters": nn_dists
    })


@pandas_udf(returnType=ArrayType(StringType()))
def osmnx_route_from_nodes(
        start_node: pd.Series, end_node: pd.Series, graph_path: pd.Series
) -> pd.Series:
    """
    Generate routes from start/end nodes
    - start_node, end_node, graph_path are all Series
    - uses a started_at of 0 to then use relative time deltas for the route
    """
    from geopy.distance import geodesic
    import osmnx as ox
    import numpy as np

    pdf = pd.DataFrame({
        "start_node": start_node,
        "end_node": end_node,
        "graph_path": graph_path
    })
    pdf_grouped = pdf.groupby('graph_path')

    # iterate over each group
    # - start time is 0 for all routes
    result = []
    start_ts = 0
    for graph_path, pdf_data in pdf_grouped:
        start_nodes = pdf_data['start_node']
        end_nodes = pdf_data['end_node']

        # load the graphml file for each group
        G = ox.load_graphml(filepath=graph_path)

        # get the shortest path for each node pair
        all_routes = ox.shortest_path(G, start_nodes, end_nodes, weight="travel_time")
        all_coords = []
        all_timedeltas = []

        # break down the routes for timedeltas
        for i in range(len(all_routes)):
            route = all_routes[i]
            route_coords = []
            route_timedeltas = []
            if route is not None:
                for j in range(len(route)):
                    if j > 0 and 'geometry' in G[route[j - 1]][route[j]][0]:
                        sub_coords = [(x[0], x[1]) for x in G[route[j - 1]][route[j]][0]['geometry'].coords]
                        route_coords += sub_coords
                route_timedeltas = [0] + [geodesic(route_coords[j - 1], route_coords[j]).m / 1.3 for j in
                                          range(len(route_coords)) if j > 0]

                # clean the coords and timedelta where repeating nodes at beginning/end of group
                route_coords = [x for i, x in enumerate(route_coords) if i != 0 and route_timedeltas[i] != 0]
                route_timedeltas = [y for i, y in enumerate(route_timedeltas) if i != 0 and y != 0]

                # create cumulative timedeltas so you can add the seconds at every step
                cumulative_timedeltas = np.cumsum(route_timedeltas).tolist()
            else:
                route_coords = []
                cumulative_timedeltas = []
            all_coords.append(route_coords)
            all_timedeltas.append(cumulative_timedeltas)

        for i in range(len(all_coords)):
            row = [f"{all_coords[i][j][0]},{all_coords[i][j][1]},{all_timedeltas[i][j]},{j}" for j in
                   range(len(all_coords[i]))]
            result.append(row)  # <- add to result

    return pd.Series(result)