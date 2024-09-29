import math

import pandas as pd
import pyproj
import shapely

from shapely import Geometry, MultiPolygon, Polygon, MultiLineString, LineString, MultiPoint, Point, GeometryCollection
from shapely import ops
from shapely.validation import explain_validity as shapely_explain_validity
from shapely.validation import make_valid as shapely_make_valid
from typing import Any, List

__all__ = [
    "get_point_z",
    "has_node_startswith",
    "has_valid_coordinates",
    "test_valid_coordinates",
    "try_buffer",
    "try_buffer_loop",
    "try_concave_hull",
    "try_explain_validity",
    "try_make_valid",
    "try_multi_flatten",
    "try_unary_union",
    "try_update_srid",
    # - helpers -
    "try_to_ewkb",
    "try_to_shapely",
    "try_to_wkt",
    "try_to_geojson"
]

def get_point_z(geom: Any) -> float:
    """
    Try to get the Z value of a point.

    :param geom: could be anything.
    :return: float value or math.NaN.
    """
    try:
        g = try_to_shapely(geom)
        return shapely.get_z(g)
    except:
        return math.nan

def has_node_startswith(geom: Any, x: float, y: float) -> bool:
    """
    Does any geom node contain x and y?
    - This is for troubleshooting which geom is throwing an error,
      e.g. non-noded intersection.
    - It matches based on string startswith, not on precise value.

    :param geom: could be anything.
    :param x: longitude
    :param y: latitude
    :return: True if found; otherwise False
    """
    try:
        g = try_to_shapely(geom)
        x_str = str(x)
        y_str = str(y)
        if isinstance(g, MultiPolygon):
            for _g in list(g.geoms):
                for ring_num, coords in enumerate(shapely.geometry.mapping(_g)['coordinates']):
                    for x, y in coords:
                        if str(x).startswith(x_str) and str(y).startswith(y_str):
                            return True
        elif isinstance(g, Polygon):
            for ring_num, coords in enumerate(shapely.geometry.mapping(g)['coordinates']):
                for x, y in coords:
                    if str(x).startswith(x_str) and str(y).startswith(y_str):
                        return True
        elif isinstance(g, MultiLineString):
            for _g in list(g.geoms):
                for r in enumerate(shapely.geometry.mapping(_g)['coordinates']):
                    x, y = r[1] # <- coords
                    if str(x).startswith(x_str) and str(y).startswith(y_str):
                        return True
        elif isinstance(g, LineString):
            for r in enumerate(shapely.geometry.mapping(g)['coordinates']):
                x, y = r[1] # <- coords
                if str(x).startswith(x_str) and str(y).startswith(y_str):
                    return True
        elif isinstance(g, MultiPoint):
            for _g in list(g.geoms):
                coords = shapely.geometry.mapping(_g)['coordinates']
                x, y = coords
                if str(x).startswith(x_str) and str(y).startswith(y_str):
                    return True
        elif isinstance(g, Point):
            coords = shapely.geometry.mapping(g)['coordinates']
            x, y = coords
            if str(x).startswith(x_str) and str(y).startswith(y_str):
                return True
    except:
        pass
    return False

def has_valid_coordinates(geom: Any) -> bool:
    """
    Checks if all points in geom are valid with respect its existing crs bounds.
    - if the geom doesn't have a srid > 0, then False will be returned

    :param geom: could be anything.
    :return: True if coordinates valid; otherwise False.
    """
    try:
        g = try_to_shapely(geom)
        gx_min, gy_min, gx_max, gy_max = shapely.bounds(g)

        crs = pyproj.CRS.from_epsg(shapely.get_srid(g))
        x_min, y_min, x_max, y_max = crs.area_of_use.bounds

        return gx_min >= x_min and gy_min >= y_min and gx_max <= x_max and gy_max <= y_max
    except:
        pass
    return False

def test_valid_coordinates(geom: Any, test_srid: int) -> bool:
    """
    Checks if all points in geom are / would be valid with respect to the provided srid bounds.
    - geom will be projected into the provided srid and tested.
    - consider passing an ewkb which will preserve the (from) srid and
      allow a projection; otherwise, you just test the current geom against the test_srid bounds.

    :param geom: could be anything.
    :param test_srid: project to srid.
    :return: True if coordinates valid; otherwise False.
    """
    try:
        g = try_to_shapely(geom)
        g1 = try_update_srid(g, 0, test_srid)
        return has_valid_coordinates(g1)
    except:
        pass
    return False

def try_buffer(geom: Any, radius: float, cap_style = 'round') -> Geometry:
    """
    Attempt to buffer a geometry and optionally alter the cap_style.
    - Sets the srid on the result from the provided geometry.

    :param geom: could be anything.
    :param radius: distance to buffer
    :param cap_style: {‘round’, ‘square’, ‘flat’}, default ‘round’
    :return: shapely Geometry or None
    """
    try:
        g = try_to_shapely(geom)
        srid = shapely.get_srid(g)
        result = shapely.buffer(g, radius, cap_style=cap_style)
        return shapely.set_srid(result, srid)
    except:
        pass
    # None returned

def try_buffer_loop(geom: Any, inner_radius: float, outer_radius: float, cap_style = 'round') -> Geometry:
    """
     Attempt to buffer a geometry using inner and outer radius and optionally alter the cap_style.
     - Sets the srid on the result from the provided geometry.

    :param geom: could be anything.
    :param inner_radius: inner distance to buffer.
    :param outer_radius: outer distance to buffer.
    :param cap_style: {‘round’, ‘square’, ‘flat’}, default ‘round’
    :return: shapely Geometry or None
    """
    try:
        g = try_to_shapely(geom)
        srid = shapely.get_srid(g)
        g_outer = try_buffer(g, outer_radius, cap_style=cap_style)
        g_inner = try_buffer(g, inner_radius, cap_style=cap_style)
        result = g_outer.difference(g_inner)
        return shapely.set_srid(result, srid)
    except:
        pass
   # None returned

def try_concave_hull(geom: Any, ratio: float, allow_holes: bool = False) -> Geometry:
    """
    Computes a concave geometry that encloses an input geometry.
    - Sets the srid on the result from the provided geometry.

    :param geom: could be anything.
    :param ratio: Number in the range [0, 1]. Higher numbers will include fewer vertices in the hull.
    :param allow_holes: whether the hull has holes, default False.
    :return: shapely Geometry or None
    """
    try:
        g = try_to_shapely(geom)
        srid = shapely.get_srid(g)
        result = shapely.concave_hull(g, ratio, allow_holes=allow_holes)
        return shapely.set_srid(result, srid)
    except:
        pass
    # None returned

def try_explain_validity(geom: Any) -> str:
    """
    Explanation of validity or invalidity.
    - This can be more expensive, so recommend
      only calling after `st_isvalid` (available in product) returns False.

    :param geom: could be anything.
    :return: explanation or None
    """
    try:
        return shapely_explain_validity(try_to_shapely(geom))
    except:
        pass
    # None returned

def try_make_valid(geom: Any) -> Geometry:
    """
    Test for geom being valid; if invalid:
    - Attempts to make valid.
    - May have to change type, e.g. POLYGON to MULTIPOLYGON.
    - Sets the new geometry to the provided srid.

    :param geom: could be anything.
    :return: shapely Geometry (orig or made valid) or None
    """
    try:
        g = try_to_shapely(geom)
        srid = shapely.get_srid(g)
        if g.is_valid:
            return g
        result = shapely_make_valid(g)
        return shapely.set_srid(result, srid)
    except:
        pass
    # None returned

def try_multi_flatten(geom: Any) -> List[Geometry]:
    """
    Purpose is to flatten a provided multi geometry into a list.
    - Handles geometry collections as well.
    - Handles non-multi as well but just added them to the result.
    - Sets each of the flattened geometry to the provided srid.

    :param geom: can be anything.
    :return: list of shapely Geometries or None
    """
    g = try_to_shapely(geom)
    try:
        # - handle srid
        srid = shapely.get_srid(g)

        # - handle GeometryCollection
        in_geoms = []
        if isinstance(g, GeometryCollection):
            in_geoms = g.geoms
        elif g:
            in_geoms = [g]

        # - handle each geom
        result = []
        for in_geom in in_geoms:
            # - handle multi
            try:
                for _g in in_geom.geoms:
                    result.append(shapely.set_srid(_g, srid))
            except:
                # - handle non-multi
                result.append(shapely.set_srid(in_geom, srid))
        if result:
            return result
    except:
        pass
    # None returned

def try_unary_union(geom_or_list: Any) -> Geometry:
    """
    Returns a geometry that represents the point set union of the given geometry.
    - Accepts an individual geometry or a list of geometries.
    - Will flatten any multi geometries or geometry collection into non-multi,
      prior to performing the union.
    - Sets srid based on first non-zero value found; otherwise it is 0.

    :param geom_or_list: individual or list like, could be anything.
    :return: shapely Geometry or None
    """
    try:
        # - handle geom or list
        geom = try_to_shapely(geom_or_list)
        if not geom:
            return __try_unary_union_list(geom_or_list)

        # - handle srid and multi-geoms
        srid = shapely.get_srid(geom)
        out_geom = shapely.unary_union(try_multi_flatten(geom))
        return shapely.set_srid(out_geom, srid)
    except:
        pass
    # None returned

def __try_unary_union_list(geom_list: List[Any]) -> Geometry:
    """
    Returns a geometry that represents the point set union of the given geometry.
    - Accepts an individual geometry or a list of geometries.
    - Will flatten any multi geometries or geometry collection into non-multi,
      prior to performing the union.
    - Sets srid based on first non-zero value found; otherwise it is 0.

    :param geom_list: list like, could be anything.
    :return: shapely Geometry or None
    """
    try:
        # - handle geom or list
        in_geoms = try_to_shapely_list(geom_list)

        # - handle srid and multi-geoms
        unary_geoms = []
        srid = 0
        for g in in_geoms:
            if not srid:
                srid = shapely.get_srid(g)
            unary_geoms.extend(try_multi_flatten(g))
        out_geom = shapely.unary_union(unary_geoms)
        return shapely.set_srid(out_geom, srid)
    except:
        pass
    # None returned

def try_update_srid(geom: Any, from_srid: int, to_srid: int) -> Geometry:
    """
    Transform a geometry from provided CRS to specified CRS.
    - Input can be WKT, WKB, GeoJSON, or shapely Geometry.
    - to_srid will be just set (if it is > 0) when the following conditions met:
      (a) from_srid is <= 0 and
      (b) provided geom srid is <= 0

    :param geom: could be anything.
    :param from_srid: e.g. 4326
    :param to_srid: e.g. 3857
    :return: transformed as shapely Geometry or None
    """
    try:
        g = try_to_shapely(geom)
        srid = shapely.get_srid(g)
        if from_srid is not None and from_srid > 0:
            srid = from_srid
        if srid > 0 and to_srid is not None and to_srid > 0:
            crs1 = pyproj.CRS.from_epsg(srid)
            crs2 = pyproj.CRS.from_epsg(to_srid)
            project = pyproj.Transformer.from_crs(crs1, crs2, always_xy=True).transform

            g1 = ops.transform(project, g)
            shapely.set_srid(g1, to_srid)
            return g1
        elif to_srid is not None and to_srid > 0:
            return shapely.set_srid(g, to_srid)
    except:
        pass
    # None returned

############################################
# HELPERS:
############################################

def try_to_shapely(g: Any) -> Geometry:
    """
    Convert WKT, WKB, or GeoJSON to Shapely.
    - Returns shapely Geometry if that is what is provided.
    - Attempts string, bytes and bytes to string variations.
    - Returns None if unsuccessful.

    :param g: could be anything.
    :return: shapely Geometry or None
    """
    try:
        if isinstance(g, Geometry):
            return g
        elif isinstance(g, str):
            try:
                return shapely.from_wkt(g)
            except:
                pass
            try:
                return shapely.from_geojson(g)
            except:
                pass
            return shapely.from_wkb(g)  # <- wkb hex str?
        else:
            try:
                return shapely.from_wkb(g)  # <- wkb binary
            except:
                pass
            try:
                return shapely.from_wkt(g.decode())
            except:
                pass
            try:
                return shapely.from_geojson(g.decode())
            except:
                pass
            return shapely.from_wkb(g.decode())  # <- wkb hex bytes?
    except:
        pass
    # None returned

def try_to_shapely_list(geoms: Any) -> List[Geometry]:
    try:
        result = []
        for _g in geoms:
            result.append(try_to_shapely(_g))
        return result
    except:
        pass
    # None returned

def try_to_ewkb(g: Any, srid: int = None) -> bytes:
    """
    Convert WKT, WKB, or GeoJSON or shapely Geometry to EWKB.
    - Attempts string, bytes and bytes to string variations.
    - Returns None if unsuccessful.

    :param g: could be anything.
    :param srid: int to set (if >= 0).
    :return: ewkb (as bytes) or None
    """
    try:
        g1 = try_to_shapely(g)
        if srid is not None and srid >= 0:
            g1 = shapely.set_srid(g1, srid)
        return shapely.to_wkb(g1, include_srid=True)
    except:
        pass
    # None returned

def try_to_wkt(g: Any, to_4326: bool = False, from_srid: int = 0) -> str:
    """
    Convert WKT, WKB, or GeoJSON or shapely Geometry to WKT.
    - Attempts string, bytes and bytes to string variations.
    - Returns None if unsuccessful.

    :param g: could be anything.
    :param to_4326: if srid > 0.
    :param from_srid: default 0.
    :return: wkt (str) or None.
    """
    try:
        g1 = try_to_shapely(g)
        srid = shapely.get_srid(g1)
        if from_srid > 0:
            srid = from_srid
        if to_4326 and srid is not None and srid > 0:
            g1 = try_update_srid(g1, srid, 4326)
        return shapely.to_wkt(g1)
    except:
        pass
    # None returned

def try_pandas_to_wkt(pdf: pd.DataFrame, geom_col: str, to_4326: bool, from_srid: int = 0) -> pd.DataFrame:
    """
    Convert the geom column to wkt.

    :param pdf: pandas DataFrame.
    :param geom_col: column to convert.
    :param to_4326: whether to convert to WGS84.
    :param from_srid: default 0.
    :return: pandas DataFrame.
    """
    pdf[geom_col] = pdf[geom_col].apply(try_to_wkt, to_4326=to_4326, from_srid=from_srid)
    return pdf

def try_to_geojson(g: Any, to_4326: bool = False, from_srid: int = 0) -> str:
    """
    Convert WKT, WKB, or GeoJSON or shapely Geometry to GeoJSON.
    - Attempts string, bytes and bytes to string variations.
    - Returns None if unsuccessful.

    :param g: could be anything.
    :param to_4326: if srid > 0.
    :param from_srid: default 0.
    :return: ewkb as bytes or None.
    """
    try:
        g1 = try_to_shapely(g)
        srid = shapely.get_srid(g1)
        if from_srid > 0:
            srid = from_srid
        if to_4326 and srid is not None and srid > 0:
            g1 = try_update_srid(g1, srid, 4326)
        return shapely.to_geojson(g1)
    except:
        pass
    # None returned