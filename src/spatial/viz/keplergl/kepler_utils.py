__all__ = [
    "KeplerViz"
]

from importlib import import_module
from pyspark.sql import DataFrame
from typing import Any

from pyspark.sql.connect.session import SparkSession
from spatial.vector import vector_utils
from ..viz_helpers import *

import re

kepler_height = 800
kepler_width = 1200

class KeplerViz:

    def __init__(self, spark: SparkSession):
        self.spark = spark
        try:
            self.KeplerGl = getattr(import_module('keplergl'), 'KeplerGl')
        except ImportError:
            print(
                "Optional dependency 'keplergl' not installed, "
                "e.g. call `pip install databricks-spatial[viz]` to install."
            )
        else:
            globals()["KeplerGl"] = self.KeplerGl

    @staticmethod
    def kepler_html(kmap: Any, height: int = kepler_height, width: int = kepler_width) -> str:
        """
        Convenience function to render map in kepler.gl.

        :param kmap: KeplerGL instance
        :param height: display height
        :param width: display width
        :return: html to render, e.g. with `displayHTML` in Databricks
        """
        if not isinstance(kmap, globals()["KeplerGl"]):
            print("Param `kmap` is not an instance of KeplerGl, nothing to do.")
            return None

        decoded: str = (
            kmap._repr_html_()
            .decode("utf-8")
            .replace(".height||400", f".height||{height}")
            .replace(".width||400", f".width||{width}")
        )

        ga_script_redacted = re.sub(
            r"\<script\>\(function\(i,s,o,g,r,a,m\).*?GoogleAnalyticsObject.*?(\<\/script\>)",
            "",
            decoded,
            flags=re.DOTALL,
        )
        del decoded

        async_script_redacted = re.sub(
            r"s\.a\.createElement\(\"script\",\{async.*?\}\),",
            "",
            ga_script_redacted,
            flags=re.DOTALL,
        )
        del ga_script_redacted

        return async_script_redacted

    def map_render_items(self, *df_map_items: DFMapItem,
                                override_zoom_info: ZoomInfo = None,
                                kepler_map_style: str = 'dark',
                                debug_level: int = 0) -> str:
        """
        Calls `display_kepler` using conventions.
        - Calculates center lat/lon and zoom level [based on first layer passed],
          if override ZoomInfo not specified
        - Renders one or more passed Spark DFMapItems,
          each will be a separate layer
        - Must specify render col and RENDER_TYPE
        - Can use specified layer name in DFMapItem;
          otherwise, will be generated
        - Can specify a render sample limit in DFMapItem
        - Can specify a zoom calc sample limit in DFMapItem;
          otherwise it will be all

          :param df_map_items: DFMapItem
          :param override_zoom_info: ZoomInfo
          :param kepler_map_style: default 'dark'
          :param debug_level: 0+, default 0
          :return: html to render, e.g. with `displayHTML` in Databricks
        """
        layers = {}
        zoom_info = DEFAULT_ZOOM_INFO
        if override_zoom_info is not None:
            zoom_info = override_zoom_info

        for layer_num, df_map_item in enumerate(df_map_items):
            # - zoom info [first layer]
            if layer_num == 0 and override_zoom_info is None:
                zoom_info = calc_zoom_info(self.spark, df_map_item, debug_level=debug_level)

            # - layer name
            layer_name = df_map_item.layer_name
            if layer_name is None:
                layer_name = f"layer_{layer_num}"

            # - data
            if df_map_item.render_type in [RENDER_TYPE.GEOMETRY]:
                geo_format = df_map_item.geo_format
                pdf = (
                    df_map_item
                       .df
                       .drop(*df_map_item.exclude_cols)
                       .toPandas()
                )
                if df_map_item.from_srid == 0 and geo_format is not None and geo_format in [GEO_FORMAT.WKT]:
                    # - use WKT directly
                    layers[layer_name] = pdf
                else:
                    # - convert to WKT (EWKB can get transformed; also via from_srid)
                    layers[layer_name] = vector_utils.try_pandas_to_wkt(
                        pdf,
                        df_map_item.render_col,
                        to_4326=True,
                        from_srid=df_map_item.from_srid
                    )
            elif df_map_item.render_type in [RENDER_TYPE.H3_STR]:
                layers[layer_name] = (
                    df_map_item
                       .df
                       .drop(*df_map_item.exclude_cols)
                       .toPandas()
                )
            elif df_map_item.render_type == RENDER_TYPE.H3_INT:
                layers[layer_name] = (
                    df_map_item
                    .df
                    .selectExpr(
                        f"h3_h3tostring({df_map_item.render_col}) as {df_map_item.render_col}",
                        f"* except({df_map_item.render_col})"
                    )
                    .drop(*df_map_item.exclude_cols)
                    .toPandas()
                )

        # - return is important here
        return KeplerViz.kepler_html(
            self.KeplerGl(
                config={
                    'version': 'v1',
                    'mapState': {
                        'longitude': zoom_info.map_x,
                        'latitude': zoom_info.map_y,
                        'zoom': zoom_info.map_zoom
                    },
                    'mapStyle': {'styleType': kepler_map_style},
                    'options': {'readOnly': False, 'centerMap': True}
                },
                data=layers,
                show_docs=False,
            )
        )

    def map_render(self, df: DataFrame, geom_col: str, geo_format: GEO_FORMAT = GEO_FORMAT.MISSING, exclude_cols: list = [],
                   override_zoom_info: ZoomInfo = None, kepler_map_style='dark', debug_level: int = 0) -> str:
        """
        Render a Spark Dataframe, using geometry col for center and zoom,
        if overrides not specified.

        :param df: DataFrame
        :param geom_col: name of column to render
        :param geo_format: GEO_FORMAT, default is MISSING
        :param exclude_cols: list of columns to explude, default is []
        :param override_zoom_info: ZoomInfo, default is None
        :param kepler_map_style: default is 'dark'
        :param debug_level: 0+, default is 0
        :return: html to render, e.g. with `displayHTML` in Databricks
        """
        return self.map_render_items(
            DFMapItem(df, geom_col, RENDER_TYPE.GEOMETRY, geo_format=geo_format, exclude_cols=exclude_cols),
            override_zoom_info=override_zoom_info,
            kepler_map_style=kepler_map_style,
            debug_level=debug_level)
