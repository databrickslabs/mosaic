__all__ = [
    "DFMapItem",
    "ZoomInfo",
    "DEFAULT_ZOOM_INFO",
    "RENDER_TYPE",
    "GEO_FORMAT",
    "calc_zoom_info",
    "render_gdf"
]

from dataclasses import dataclass
from dataclasses import field
from enum import Enum

from pyspark.sql import DataFrame
from pyspark.sql.connect.session import SparkSession
from spatial.vector import vector_utils

import geopandas as gpd

class RENDER_TYPE(Enum):
    """Specify expected type of 'render_col' in DFMapItem."""
    GEOMETRY = 100
    H3_INT = 200
    H3_STR = 300

class GEO_FORMAT(Enum):
    """Specify expected type of 'render_col' in DFMapItem [when RENDER_TYPE = GEOMETRY]."""
    MISSING = 0
    WKT = 1
    WKB = 2
    GEOJSON = 3
    EWKB = 4

@dataclass
class DFMapItem:
    """Class for holding some properties for rendering a map."""
    df: DataFrame
    render_col: str
    render_type: RENDER_TYPE
    geo_format: GEO_FORMAT = GEO_FORMAT.MISSING
    from_srid: int = 0
    layer_name: str = None
    zoom_calc_sample_limit: int = None
    exclude_cols: list = field(default_factory=list)

@dataclass
class ZoomInfo:
    map_x: float
    map_y: float
    map_zoom: float

DEFAULT_ZOOM_INFO = ZoomInfo(0.0, 0.0, 3.0)


def calc_zoom_info(spark: SparkSession, df_map_item: DFMapItem, debug_level: int = 0) -> ZoomInfo:
    """
    Example output of debug_level=1
    {'xmin': -100.5, 'ymin': 50.05, 'xmax': -100.25, 'ymax': 50.5, 'centroid_x': -100.375, 'centroid_y': 50.275, 'pnt_sw': 'POINT(-100.5 50.05)', 'pnt_nw': 'POINT(-100.5 50.5)', 'pnt_se': 'POINT(-100.25 50.05)', 'pnt_ne': 'POINT(-100.25 50.5)',  'width_meters': 17905.33401827115, 'height_meters': 50055.461462782696, 'max_meters': 50055.461462782696, 'zoom': 9.5}
    :param spark: SparkSession
    :param df_map_item: DFMapItem
    :param debug_level: 0+, default 0.
    :return: ZoomInfo
    """

    from pyspark.sql import functions as F

    # - handle zoom sample
    # - go ahead and drop exclude cols, if any
    df_samp = df_map_item.df.drop(*df_map_item.exclude_cols)
    samp_limit = df_map_item.zoom_calc_sample_limit
    if samp_limit is not None:
        cnt = df_samp.count()
        if samp_limit < cnt:
            df_samp = (
                df_samp
                .dropna(df_map_item.render_col)
                .sample(float(samp_limit) / float(cnt))
                .limit(samp_limit)
            )

    # - handle h3
    srid = df_map_item.from_srid
    geom_col = df_map_item.render_col
    geo_format = df_map_item.geo_format
    if df_map_item.render_type in [RENDER_TYPE.H3_INT, RENDER_TYPE.H3_STR]:
        srid = 0
        geom_col = "h3_geom"
        geo_format = GEO_FORMAT.WKB
        df_samp = df_samp.withColumn(geom_col, F.expr(f"h3_boundaryaswkb({df_map_item.render_col})"))

    # standardize to SRID=4326
    if srid > 0 or (geo_format is not None and geo_format not in [GEO_FORMAT.WKT]):
        # - if from_srid = 0 and WKT is the format, this can be skipped.
        df_samp = spark.createDataFrame(
            vector_utils.try_pandas_to_wkt(
                df_samp.toPandas(),
                geom_col,
                to_4326=True,
                from_srid=srid
            )
        )

    d = (
        df_samp
        # - xy min/max
        .select(
            F.expr(f"st_xmin({geom_col}) as xmin"),
            F.expr(f"st_ymin({geom_col}) as ymin"),
            F.expr(f"st_xmax({geom_col}) as xmax"),
            F.expr(f"st_ymax({geom_col}) as ymax")
        )
        .groupBy()
        .agg(
            F.min("xmin").alias("xmin"),
            F.min("ymin").alias("ymin"),
            F.max("xmax").alias("xmax"),
            F.max("ymax").alias("ymax")
        )
        # - centroid xy ranges
        .withColumn("centroid_x", F.expr("(xmin + xmax) / 2.0"))
        .withColumn("centroid_y", F.expr("(ymin + ymax) / 2.0"))
        .withColumn("pnt_sw", F.expr("st_astext(st_point(xmin,ymin))"))
        .withColumn("pnt_nw", F.expr("st_astext(st_point(xmin,ymax))"))
        .withColumn("pnt_se", F.expr("st_astext(st_point(xmax,ymin))"))
        .withColumn("pnt_ne", F.expr("st_astext(st_point(xmax,ymax))"))
        .withColumn(
            "width_meters",
            F.expr(
                "st_geoglength(st_astext(st_makeline(array( st_geomfromtext(pnt_sw), st_geomfromtext(pnt_se) ))))")
        )
        .withColumn(
            "height_meters",
            F.expr(
                "st_geoglength(st_astext(st_makeline(array( st_geomfromtext(pnt_sw), st_geomfromtext(pnt_nw) ))))")
        )
        .withColumn(
            "max_meters",
            F
            .when(F.expr("width_meters >= height_meters"), F.col("width_meters"))
            .otherwise(F.col("height_meters"))
        )
        # - zoom
        # https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Resolution_and_Scale
        # 1cm = ~.4in
        # assume 16cm = ~6in (height of viewport)
        # but mapbox tiles are 512px instead of 256px, so divide by 2 [h=8 tiles]
        .withColumn(
            "zoom",
            F
            .when(F.expr("max_meters < 21.2 * 8"), F.lit(18))
            .when(F.expr("max_meters < 42.3 * 8"), F.lit(17))
            .when(F.expr("max_meters < 84.6 * 8"), F.lit(16))
            .when(F.expr("max_meters < 169 * 8"), F.lit(15))
            .when(F.expr("max_meters < 339 * 8"), F.lit(14))
            .when(F.expr("max_meters < 677 * 8"), F.lit(13))
            .when(F.expr("max_meters < 1.35 * 1000 * 8"), F.lit(12))
            .when(F.expr("max_meters < 2.7  * 1000 * 8"), F.lit(11))
            .when(F.expr("max_meters < 5.4  * 1000 * 8"), F.lit(10))
            .when(F.expr("max_meters < 10.8 * 1000 * 8"), F.lit(9))
            .when(F.expr("max_meters < 21.7 * 1000 * 8"), F.lit(8))
            .when(F.expr("max_meters < 43.3 * 1000 * 8"), F.lit(7))
            .when(F.expr("max_meters < 86.7 * 1000 * 8"), F.lit(6))
            .when(F.expr("max_meters < 173  * 1000 * 8"), F.lit(5))
            .when(F.expr("max_meters < 347  * 1000 * 8"), F.lit(4))
            .when(F.expr("max_meters < 693  * 1000 * 8"), F.lit(3))
            .when(F.expr("max_meters < 1387 * 1000 * 8"), F.lit(2))
            .when(F.expr("max_meters < 2773 * 1000 * 8"), F.lit(1))
            .otherwise(F.lit(0))
        )
    ).first().asDict()

    (debug_level > 0) and print(d, "\n")
    return ZoomInfo(d['centroid_x'], d['centroid_y'], d['zoom'])


def render_gdf(
        gdf: gpd.GeoDataFrame, val_col:str=None, figsize: tuple[int, int]=(10, 10), kwargs: dict = {}
):
    """Render a Geopandas dataframe using its 'geometry' column.

    - this can be used to just render geometries
    - can also be used to render h3 tessellated rasters

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        The geodataframe to plot
    val_col : str (optional)
        If provided, this will be used to color the plot, e.g. measure;
        default is None; ignored if other color params are provided
    figsize : tuple[int, int] (optional)
        The size of the figure, prior to any auto sizing;
        default is (10,10)
    kwargs : dict
        Any additional args; they will be passed directly to the underlying
        function as '**kwargs'
    Returns
    -------
    ax
        matplotlib axes instance
    """
    gdf.plot(column=val_col, figsize=figsize, **kwargs)