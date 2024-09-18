import pyproj
import shapely

from shapely import Geometry, MultiPolygon, Polygon
from shapely import ops
from shapely.validation import explain_validity as shapely_explain_validity
from shapely.validation import make_valid as shapely_make_valid
from typing import List

__all__ = [
    "explain_validity",
    "flatten_polygons",
    "is_poly_node",
    "make_valid",
    "transform"
]

def explain_validity(geom: Geometry) -> str:
    """
    Explanation of validity or invalidity.
    - This can be more expensive, so recommend
      only calling after `st_isvalid` returns False.

    :param geom: the geometry to explain
    :return: explanation
    """
    return shapely_explain_validity(geom)

def flatten_polygons(multipoly: MultiPolygon) -> List[Polygon]:
    """
    Flattens a MultiPolygon into a list of polygons.

    :param geom: the input multi-polygon
    :return: list of polygons or empty list
    """
    return multipoly.geoms

def is_poly_node(geom: Geometry, x: float, y: float) -> bool:
    """
    Does any geom node contain x and y?
    - This is for troubleshooting which geom is throwing an error,
      e.g. non-noded intersection.
    - It matches based on string startswith, not on precise value.

    :param geom: shapely geometry
    :param x: longitude
    :param y: latitude
    :return: True if found; otherwise False
    """
    x_str = str(x)
    y_str = str(y)
    if isinstance(geom, shapely.geometry.Polygon):
        for ring_num, coords in enumerate(shapely.geometry.mapping(geom)['coordinates']):
            for x, y in coords:
                if str(x).startswith(x_str) and str(y).startswith(y_str):
                    return True
    elif isinstance(geom, shapely.geometry.MultiPolygon):
        for _g in list(geom.geoms):
            for ring_num, coords in enumerate(shapely.geometry.mapping(_g)['coordinates']):
                for x, y in coords:
                    if str(x).startswith(x_str) and str(y).startswith(y_str):
                        return True
    return False

def make_valid(geom: Geometry) -> Geometry:
    """
    Test for geom being valid; if invalid:
    - Attempts to make valid
    - May have to change type, e.g. POLYGON to MULTIPOLYGON

    :param geom: the geometry to make valid
    :return: geometry (orig or made valid)
    """
    if geom.is_valid:
        return geom
    return shapely_make_valid(geom)

def transform(geom: Geometry, from_crs: str, to_crs: str) -> Geometry:
    """
    Transform a geometry from provided CRS to specified CRS.

    :param geom: shapely geometry
    :param from_crs: e.g. 'epsg:4326'
    :param to_crs: e.g. 'epsg:3857'
    :return: transformed geometry
    """
    crs1 = pyproj.CRS(from_crs)
    crs2 = pyproj.CRS(to_crs)
    project = pyproj.Transformer.from_crs(crs1, crs2, always_xy=True).transform

    return ops.transform(project, geom).wkt