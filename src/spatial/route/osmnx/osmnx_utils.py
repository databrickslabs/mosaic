__all__ = [
    "OsmnxMgr"
]

from pyspark.sql.types import *

import pandas as pd

class OsmnxMgr:

    nearest_node_schema = StructType([
        StructField("nn", LongType()),
        StructField("nn_meters", DoubleType())
    ])

    route_cols = [
        'step',
        'node_u', 'node_v', 'osmid', 'oneway', 'lanes', 'name', 'highway',
        'maxspeed', 'reversed', 'length', 'speed_kph', 'travel_time', 'wkt'
    ]

    route_schema = ArrayType(StructType([
        StructField('step', LongType(), True),
        StructField('node_u', LongType(), True),
        StructField('node_v', LongType(), True),
        StructField('osmid', ArrayType(LongType()), True),
        StructField('oneway', BooleanType(), True),
        StructField('lanes', StringType(), True),
        StructField('name', StringType(), True),
        StructField('highway', StringType(), True),
        StructField('maxspeed', StringType(), True),
        StructField('reversed', ArrayType(BooleanType()), True),
        StructField('length', DoubleType(), True),
        StructField('speed_kph', DoubleType(), True),
        StructField('travel_time', DoubleType(), True),
        StructField('wkt', StringType(), True)
    ]))

    @staticmethod
    def graph_from_pickle(pickle_path: str):
        """
        :param pickle_path:
        :return: networkx.MultiDiGraph
        """
        import pickle

        with open(pickle_path, 'rb') as f:
            return pickle.load(f)

    @staticmethod
    def graph_to_cloudpickle(graph, pickle_path):
        """
        :param graph: networkx.MultiDiGraph
        :param pickle_path:
        """
        import cloudpickle

        with open(pickle_path, 'wb') as f:
                cloudpickle.dump(graph, f)

    @staticmethod
    def load_graph(graph_path, is_pickled: bool):
        """
        :param graph_path:
        :param is_pickled:
        :return: networkx.MultiDiGraph
        """
        import osmnx as ox

        if is_pickled:
            return OsmnxMgr.graph_from_pickle(graph_path)
        else:
            return ox.load_graphml(filepath=graph_path)

    @staticmethod
    def nearest_node(lng: pd.Series, lat: pd.Series, graph_path: pd.Series, pickled_graph: bool = False) -> pd.DataFrame:
        """
        Get nearest graph node from lng/lat coords.
        - loads / uses graphml file for the route in a "pseudo" grouped manner (maintains original index order)
        - if pickled_graph is True, expects a pickled path, default is False
        Returns Struct[<nn>,<nn_meters>] for distance of coords to nearest node.
        """
        import math
        import osmnx as ox

        # populate graph_map
        # - processing "psuedo" grouped by graphml file
        # - have to maintain the order as-provided
        # - None and NaN values are identified and handled
        pdf = pd.DataFrame({
            "lng": lng,
            "lat": lat,
            "graph_path": graph_path
        })
        graph_map = {'SKIP': []}
        for idx, row in pdf.iterrows():
            x, y = (row['lng'], row['lat'])
            if x is None or math.isnan(x) or y is None or math.isnan(y):
                graph_map['SKIP'].append(idx)
                continue
            gp = row['graph_path']
            if not gp in graph_map:
                graph_map[gp] = [idx]
            else:
                graph_map[gp].append(idx)
        del pdf

        # iterate over each "pseudo" group
        # - populate the array per index
        # - start time is 0 for all routes
        result = []
        for graph_path_key, idx_list in graph_map.items():
            # - handle skips
            if graph_path_key == 'SKIP':
                for i in idx_list:
                    result.append({"idx": i, "nn": math.nan, "nn_meters": math.nan})
                continue

            # - handle non-skips
            G = OsmnxMgr.load_graph(graph_path_key, is_pickled = pickled_graph)
            xs = lng[lng.index.isin(idx_list)]
            ys = lat[lat.index.isin(idx_list)]

            # result is a tuple of (nearest_node, distance) series
            nodes = ox.distance.nearest_nodes(G, X=xs, Y=ys, return_dist=True)

            for i in range(len(nodes[0])):
                result.append({"idx": idx_list[i], "nn": nodes[0][i], "nn_meters": nodes[1][i]})

        # return values in same idx order as provided
        pdf_result = pd.DataFrame.from_dict(result)
        pdf_result.set_index("idx", inplace=True)
        pdf_result.sort_index(inplace=True)
        pdf_result.reset_index(drop=True, inplace=True)
        return pdf_result

    @staticmethod
    def route(
            start_node: pd.Series, end_node: pd.Series, graph_path: pd.Series,
            weight: str, pickled_graph: bool = False, as_string: bool = False, cpus: int = 1
    ) -> pd.Series:
        """
        Generate routes from start/end graph nodes.
        - start_node, end_node, graph_path are all Series (None and NaN values are identified and handled)
        - loads / uses graphml file for the route in a "pseudo" grouped manner (maintains original index order)
        - if pickled_graph is True, expects a pickled path, default is False
        - weight used, e.g. 'length' or 'travel_time'
        - cpus default to 1 (match spark defaults)
        Returns each route as a list of Structs | json string which can be parsed out.
        """
        import geopandas as gpd
        import json
        import math
        import osmnx as ox

        # populate graph_map
        # - processing "psuedo" grouped by graphml file
        # - have to maintain the order as-provided
        # - None and NaN values are identified and handled
        pdf = pd.DataFrame({
            "start_node": start_node,
            "end_node": end_node,
            "graph_path": graph_path
        })
        graph_map = {'SKIP': []}
        for idx, row in pdf.iterrows():
            s, e = (row['start_node'], row['end_node'])
            if s is None or math.isnan(s) or e is None or math.isnan(e):
                graph_map['SKIP'].append(idx)
                continue
            gp = row['graph_path']
            if not gp in graph_map:
                graph_map[gp] = [idx]
            else:
                graph_map[gp].append(idx)
        del pdf

        # iterate over each "pseudo" group
        # - populate the array per index
        # - start time is 0 for all routes
        result_map = {}
        no_result = []
        if as_string:
            no_result = json.dumps(no_result)
        for graph_path_key, idx_list in graph_map.items():
            # - handle skips
            if graph_path_key == 'SKIP':
                for i in idx_list:
                    result_map[i] = no_result
                continue

            # - handle non-skips
            G = OsmnxMgr.load_graph(graph_path_key, is_pickled=pickled_graph)
            s_nodes = start_node[start_node.index.isin(idx_list)]
            e_nodes = end_node[end_node.index.isin(idx_list)]

            # get the shortest path for each node pair
            all_routes = ox.shortest_path(G, s_nodes, e_nodes, weight=weight, cpus=cpus)

            # break down the routes for timedeltas
            for i, route in enumerate(all_routes):
                try:
                    # geodataframe from route
                    gdf = ox.routing.route_to_gdf(G, route, weight=weight)

                    # osmid special handling
                    # - may be an array or a int
                    gdf['osmid'] = gdf['osmid'].apply(
                        lambda osmid: osmid if isinstance(osmid, list) else [osmid])

                    # reversed special handling
                    # - may be an array or a bool
                    gdf['reversed'] = gdf['reversed'].apply(
                        lambda reversed: reversed if isinstance(reversed, list) else [reversed])

                    # fix type for lanes, name, maxspeed
                    gdf['lanes'] = gdf['lanes'].astype(str).apply(lambda x: x if x != 'nan' else '')
                    gdf['name'] = gdf['name'].astype(str).apply(lambda x: x if x != 'nan' else '')
                    gdf['maxspeed'] = gdf['maxspeed'].astype(str).apply(lambda x: x if x != 'nan' else '')

                    # geometry special handling
                    # - spark dataframe doesn't like shapely geoms
                    gdf['wkt'] = gpd.GeoSeries.to_wkt(gdf['geometry'])
                    gdf.drop(columns=['geometry'], inplace=True)

                    # include node 'u','v'
                    # - rename as 'node_u', 'node_v'
                    gdf.reset_index(inplace=True)
                    gdf.rename(columns={'u': 'node_u', 'v': 'node_v'}, inplace=True)

                    # append the route to results
                    # - pandas udfs require 1:1 results,
                    #   so keeping as an array per row
                    # - adding row_num to support easy use of 'INLINE'
                    # - maintaining struct col ordering
                    gdf['step'] = range(len(gdf))
                    result = gdf[OsmnxMgr.route_cols].to_dict(orient='records')
                    if as_string:
                        result = json.dumps(result)
                    result_map[idx_list[i]] = result
                except:
                    result_map[idx_list[i]] = no_result
                    continue
        del graph_map

        # return values in same idx order as provided
        sorted_idx_keys = sorted(result_map.keys())
        return pd.Series([result_map[key] for key in sorted_idx_keys])