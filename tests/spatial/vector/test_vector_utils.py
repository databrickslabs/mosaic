import math

import pandas as pd
import shapely
import unittest

import vector_utils
from vector_utils import try_to_ewkb, try_to_shapely

MULTI_POLY_EXAMPLE = "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))"
MULTI_LINE_EXAMPLE = "MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))"
MULTI_POINT_EXAMPLE = "MULTIPOINT ((0 0), (1 1))"
LINE_EXAMPLE = "LINESTRING (0 0, 0 1, 1 2)"
POINT_EXAMPLE = "POINT (0 0)"
WKT_STR_EXAMPLE = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"
GEOJSON_STR_EXAMPLE = '{"type":"Polygon","coordinates":[[[30.0,10.0],[40.0,40.0],[20.0,40.0],[10.0,20.0],[30.0,10.0]]]}'
WKB_HEX_EXAMPLE = '010300000001000000050000000000000000003E4000000000000024400000000000004440000000000000444000000000000034400000000000004440000000000000244000000000000034400000000000003E400000000000002440'
WKB_BIN_EXAMPLE = b'\x01\x03\x00\x00\x00\x01\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00>@\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00D@\x00\x00\x00\x00\x00\x00D@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x00D@\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x00>@\x00\x00\x00\x00\x00\x00$@'
INVALID_EXAMPLE = "POLYGON ((5 0, 2.5 9, 9.5 3.5, 0.5 3.5, 7.5 9, 5 0))"
COLLECTION_EXAMPLE = "GEOMETRYCOLLECTION (MULTIPOINT((0 0), (1 1)), POINT(3 4), LINESTRING(2 3, 3 4))"

BNG_27700_WKT = "POINT (327420.988668 690284.547110)"
BNG_27700_EWKB = b'\x01\x01\x00\x00 4l\x00\x00Zbe\xf4\xf3\xfb\x13AK\xcd\x1e\x18\xd9\x10%A'

class TestVectorUtils(unittest.TestCase):

    ############################################
    # [1] TEST HELPERS:
    ############################################

    def test_try_to_shapely(self):
        # - test geometry
        self.assertIsInstance(
            vector_utils.try_to_shapely(shapely.from_wkt(WKT_STR_EXAMPLE)),
            shapely.Geometry,
            "Shapely obj should not be None."
        )

        # - test str
        self.assertIsNotNone(vector_utils.try_to_shapely(WKT_STR_EXAMPLE), "WKT str should not be None.")
        self.assertIsNotNone(vector_utils.try_to_shapely(GEOJSON_STR_EXAMPLE), "GEOJSON str should not be None.")
        self.assertIsNotNone(vector_utils.try_to_shapely(WKB_HEX_EXAMPLE), "WKB HEX str should not be None.")
        self.assertIsNotNone(vector_utils.try_to_shapely(WKB_BIN_EXAMPLE.decode()), "WKB bytes as str should not be None.")

        # - test binary
        self.assertIsNotNone(vector_utils.try_to_shapely(WKB_BIN_EXAMPLE), "WKB bytes should not be None.")
        self.assertIsNotNone(vector_utils.try_to_shapely(str.encode(WKT_STR_EXAMPLE)), "WKT bytes should not be None.")
        self.assertIsNotNone(vector_utils.try_to_shapely(str.encode(GEOJSON_STR_EXAMPLE)), "Geojson bytes should not be None.")
        self.assertIsNotNone(vector_utils.try_to_shapely(str.encode(WKB_HEX_EXAMPLE)), "WKB hex bytes should not be None.")

        # - test invalid
        self.assertIsNone(vector_utils.try_to_shapely(""), "Empty str input should return None.")
        self.assertIsNone(vector_utils.try_to_shapely("..."), "Non-geom str input should return None.")
        self.assertIsNone(vector_utils.try_to_shapely(None), "None input should return None.")

    def test_try_to_ewkb(self):
        # - test str
        self.assertIsNotNone(vector_utils.try_to_ewkb(WKT_STR_EXAMPLE), "WKT str should not be None.")
        self.assertIsNotNone(vector_utils.try_to_ewkb(WKT_STR_EXAMPLE, 4326), "WKT str + srid should not be None.")

        self.assertIsNotNone(vector_utils.try_to_ewkb(GEOJSON_STR_EXAMPLE), "GEOJSON str should not be None.")
        self.assertIsNotNone(vector_utils.try_to_ewkb(GEOJSON_STR_EXAMPLE, 4326), "GEOJSON str + srid should not be None.")

        self.assertIsNotNone(vector_utils.try_to_ewkb(WKB_HEX_EXAMPLE), "WKB HEX str should not be None.")
        self.assertIsNotNone(vector_utils.try_to_ewkb(WKB_HEX_EXAMPLE, 4326),"WKB HEX str + srid should not be None.")

        self.assertIsNotNone(vector_utils.try_to_ewkb(WKB_BIN_EXAMPLE.decode()), "WKB bytes as str should not be None.")
        self.assertIsNotNone(vector_utils.try_to_ewkb(WKB_BIN_EXAMPLE.decode(), 4326), "WKB bytes as str + srid should not be None.")

        # - test binary
        self.assertIsNotNone(vector_utils.try_to_ewkb(WKB_BIN_EXAMPLE), "WKB bytes should not be None.")
        self.assertIsNotNone(vector_utils.try_to_ewkb(WKB_BIN_EXAMPLE, 4326), "WKB bytes + srid should not be None.")

        self.assertIsNotNone(vector_utils.try_to_ewkb(str.encode(WKT_STR_EXAMPLE)), "WKT bytes should not be None.")
        self.assertIsNotNone(vector_utils.try_to_ewkb(str.encode(WKT_STR_EXAMPLE), 4326), "WKT bytes + srid should not be None.")

        self.assertIsNotNone(vector_utils.try_to_ewkb(str.encode(GEOJSON_STR_EXAMPLE)), "Geojson bytes should not be None.")
        self.assertIsNotNone(vector_utils.try_to_ewkb(str.encode(GEOJSON_STR_EXAMPLE), 4326),"Geojson bytes + srid should not be None.")

        self.assertIsNotNone(vector_utils.try_to_ewkb(str.encode(WKB_HEX_EXAMPLE)),"WKB HEX bytes should not be None.")
        self.assertIsNotNone(vector_utils.try_to_ewkb(str.encode(WKB_HEX_EXAMPLE), 4326),"WKB HEX bytes + srid should not be None.")

        # - test invalid
        self.assertIsNone(vector_utils.try_to_ewkb(""), "Empty str input should return None.")
        self.assertIsNone(vector_utils.try_to_ewkb("..."), "Non-geom str input should return None.")
        self.assertIsNone(vector_utils.try_to_ewkb(None), "None input should return None.")

    def test_try_to_wkt(self):
        # - test valid
        self.assertIsNotNone(vector_utils.try_to_wkt(WKT_STR_EXAMPLE), "WKT str should not be None.")
        self.assertIsNotNone(vector_utils.try_to_wkt(GEOJSON_STR_EXAMPLE), "Geojson str should not be None.")
        self.assertIsNotNone(vector_utils.try_to_wkt(WKB_BIN_EXAMPLE), "WKB bytes should not be None.")
        bng_4326 = vector_utils.try_to_wkt(BNG_27700_EWKB, to_4326=True)
        self.assertIsNotNone(bng_4326, "BNG should not be None.")
        self.assertTrue(bng_4326 == "POINT (-3.168313 56.099803)")

        # - test invalid
        self.assertIsNone(vector_utils.try_to_wkt(""), "Empty str input should return None.")
        self.assertIsNone(vector_utils.try_to_wkt("..."), "Non-geom str input should return None.")
        self.assertIsNone(vector_utils.try_to_wkt(None), "None input should return None.")

    def test_try_pandas_to_wkt(self):

        # - test valid [WKB]
        pdf_wkb = pd.DataFrame({"geom": [WKB_BIN_EXAMPLE]})
        pdf_wkb_result = vector_utils.try_pandas_to_wkt(pdf_wkb, "geom", to_4326=False)
        #print(pdf_wkb_result)
        self.assertIsNotNone(pdf_wkb_result)

        # - test valid [BNG]
        pdf_bng = pd.DataFrame({"geom": [BNG_27700_EWKB]})
        pdf_bng_result_conv = vector_utils.try_pandas_to_wkt(pdf_bng, "geom", to_4326=True)
        #print(pdf_bng_result_conv)
        self.assertIsNotNone(pdf_bng_result_conv)

        pdf_bng_result = vector_utils.try_pandas_to_wkt(pdf_bng, "geom", to_4326=False)
        #print(pdf_bng_result)
        self.assertIsNotNone(pdf_bng_result)

    def test_try_to_geojson(self):
        # - test valid
        self.assertIsNotNone(vector_utils.try_to_geojson(WKT_STR_EXAMPLE), "WKT str should not be None.")
        self.assertIsNotNone(vector_utils.try_to_geojson(GEOJSON_STR_EXAMPLE), "Geojson str should not be None.")
        self.assertIsNotNone(vector_utils.try_to_geojson(WKB_BIN_EXAMPLE), "WKB bytes should not be None.")
        bng_4326 = vector_utils.try_to_geojson(BNG_27700_EWKB, to_4326=True)
        self.assertIsNotNone(bng_4326, "BNG should not be None.")
        self.assertTrue(bng_4326 == """{"type":"Point","coordinates":[-3.16831345330385,56.099802523026106]}""")

        # - test invalid
        self.assertIsNone(vector_utils.try_to_geojson(""), "Empty str input should return None.")
        self.assertIsNone(vector_utils.try_to_geojson("..."), "Non-geom str input should return None.")
        self.assertIsNone(vector_utils.try_to_geojson(None), "None input should return None.")

    ############################################
    # [2] TEST FUNCTIONS:
    ############################################

    def test_get_point_z(self):
        # - test valid input
        self.assertTrue(vector_utils.get_point_z("POINT(0 0 0)") == 0)
        self.assertTrue(math.isnan(vector_utils.get_point_z("POINT(0 0)")))
        self.assertTrue(math.isnan(vector_utils.get_point_z(WKT_STR_EXAMPLE)))

        # - test invalid input
        self.assertTrue(math.isnan(vector_utils.get_point_z("")))
        self.assertTrue(math.isnan(vector_utils.get_point_z("...")))
        self.assertTrue(math.isnan(vector_utils.get_point_z(None)))

    def test_has_node_startswith(self):
        # - test valid input (locatable)
        self.assertTrue(vector_utils.has_node_startswith(MULTI_POLY_EXAMPLE, 40, 10))
        self.assertTrue(vector_utils.has_node_startswith(WKT_STR_EXAMPLE, 10, 20)) # <- POLYGON
        self.assertTrue(vector_utils.has_node_startswith(MULTI_LINE_EXAMPLE, 3, 3))
        self.assertTrue(vector_utils.has_node_startswith(LINE_EXAMPLE, 0, 1))
        self.assertTrue(vector_utils.has_node_startswith(MULTI_POINT_EXAMPLE, 1, 1))
        self.assertTrue(vector_utils.has_node_startswith(POINT_EXAMPLE, 0, 0))

        # - test valid input (non-locatable)
        self.assertFalse(vector_utils.has_node_startswith(MULTI_POLY_EXAMPLE, 45, 15))
        self.assertFalse(vector_utils.has_node_startswith(WKT_STR_EXAMPLE, 15, 25)) # <- POLYGON
        self.assertFalse(vector_utils.has_node_startswith(MULTI_LINE_EXAMPLE, 35, 35))
        self.assertFalse(vector_utils.has_node_startswith(LINE_EXAMPLE, 5, 15))
        self.assertFalse(vector_utils.has_node_startswith(MULTI_POINT_EXAMPLE, 15, 15))
        self.assertFalse(vector_utils.has_node_startswith(POINT_EXAMPLE, 5, 5))

        # - test invalid input
        self.assertFalse(vector_utils.has_node_startswith("", 0, 0))
        self.assertFalse(vector_utils.has_node_startswith("...", 0, 0))
        self.assertFalse(vector_utils.has_node_startswith(None, 0, 0))

    def test_has_valid_coordinates(self):
        # - test valid input (projected to 4326)
        self.assertTrue(vector_utils.has_valid_coordinates(try_to_ewkb(WKT_STR_EXAMPLE, 4326)))
        self.assertTrue(vector_utils.has_valid_coordinates(try_to_ewkb(MULTI_POLY_EXAMPLE, 4326)))
        self.assertTrue(vector_utils.has_valid_coordinates(try_to_ewkb(MULTI_LINE_EXAMPLE, 4326)))
        self.assertTrue(vector_utils.has_valid_coordinates(try_to_ewkb(MULTI_POINT_EXAMPLE, 4326)))
        self.assertTrue(vector_utils.has_valid_coordinates(try_to_ewkb(LINE_EXAMPLE, 4326)))
        self.assertTrue(vector_utils.has_valid_coordinates(try_to_ewkb(POINT_EXAMPLE, 4326)))
        self.assertTrue(vector_utils.has_valid_coordinates(try_to_ewkb(COLLECTION_EXAMPLE, 4326)))

        # - test valid input (if projected to 4326)
        self.assertTrue(vector_utils.test_valid_coordinates(WKT_STR_EXAMPLE, 4326))
        self.assertTrue(vector_utils.test_valid_coordinates(MULTI_POLY_EXAMPLE, 4326))
        self.assertTrue(vector_utils.test_valid_coordinates(MULTI_LINE_EXAMPLE, 4326))
        self.assertTrue(vector_utils.test_valid_coordinates(MULTI_POINT_EXAMPLE, 4326))
        self.assertTrue(vector_utils.test_valid_coordinates(LINE_EXAMPLE, 4326))
        self.assertTrue(vector_utils.test_valid_coordinates(POINT_EXAMPLE, 4326))
        self.assertTrue(vector_utils.test_valid_coordinates(COLLECTION_EXAMPLE, 4326))

        # - test invalid input
        self.assertFalse(vector_utils.has_valid_coordinates(INVALID_EXAMPLE))
        self.assertFalse(vector_utils.has_valid_coordinates(WKT_STR_EXAMPLE))
        self.assertFalse(vector_utils.has_valid_coordinates(MULTI_POLY_EXAMPLE))
        self.assertFalse(vector_utils.has_valid_coordinates(MULTI_LINE_EXAMPLE))
        self.assertFalse(vector_utils.has_valid_coordinates(MULTI_POINT_EXAMPLE))
        self.assertFalse(vector_utils.has_valid_coordinates(LINE_EXAMPLE))
        self.assertFalse(vector_utils.has_valid_coordinates(POINT_EXAMPLE))
        self.assertFalse(vector_utils.has_valid_coordinates(COLLECTION_EXAMPLE))

    def test_try_buffer(self):
        # - test valid input (buffer)
        self.assertIsNotNone(vector_utils.try_buffer(WKT_STR_EXAMPLE, 0.1))
        self.assertIsNotNone(vector_utils.try_buffer(MULTI_POLY_EXAMPLE, 0.1))
        self.assertIsNotNone(vector_utils.try_buffer(MULTI_LINE_EXAMPLE, 0.1))
        self.assertIsNotNone(vector_utils.try_buffer(MULTI_POINT_EXAMPLE, 0.1))
        self.assertIsNotNone(vector_utils.try_buffer(LINE_EXAMPLE, 0.1))
        self.assertIsNotNone(vector_utils.try_buffer(POINT_EXAMPLE, 0.1))
        self.assertIsNotNone(vector_utils.try_buffer(COLLECTION_EXAMPLE, 0.1))

        # - test valid input (buffer_loop)
        self.assertIsNotNone(vector_utils.try_buffer_loop(WKT_STR_EXAMPLE, 0.1, 0.2))
        self.assertIsNotNone(vector_utils.try_buffer_loop(MULTI_POLY_EXAMPLE, 0.1, 0.2))
        self.assertIsNotNone(vector_utils.try_buffer_loop(MULTI_LINE_EXAMPLE, 0.1, 0.2))
        self.assertIsNotNone(vector_utils.try_buffer_loop(MULTI_POINT_EXAMPLE, 0.1, 0.2))
        self.assertIsNotNone(vector_utils.try_buffer_loop(LINE_EXAMPLE, 0.1, 0.2))
        self.assertIsNotNone(vector_utils.try_buffer_loop(POINT_EXAMPLE, 0.1, 0.2))
        self.assertIsNotNone(vector_utils.try_buffer_loop(COLLECTION_EXAMPLE, 0.1, 0.2))

        # - test invalid input
        self.assertIsNone(vector_utils.try_buffer("...", 0.1))
        self.assertIsNone(vector_utils.try_buffer_loop("...", 0.1, 0.2))

    def test_concave_hull(self):
        # - test valid input
        self.assertIsNotNone(vector_utils.try_concave_hull(WKT_STR_EXAMPLE, 0.5))
        self.assertIsNotNone(vector_utils.try_concave_hull(MULTI_POLY_EXAMPLE, 0.5))
        self.assertIsNotNone(vector_utils.try_concave_hull(MULTI_LINE_EXAMPLE, 0.5))
        self.assertIsNotNone(vector_utils.try_concave_hull(MULTI_POINT_EXAMPLE, 0.5))
        self.assertIsNotNone(vector_utils.try_concave_hull(LINE_EXAMPLE, 0.5))
        self.assertIsNotNone(vector_utils.try_concave_hull(POINT_EXAMPLE, 0.5))
        self.assertIsNotNone(vector_utils.try_concave_hull(COLLECTION_EXAMPLE, 0.5))

        # - test invalid input
        self.assertIsNone(vector_utils.try_concave_hull("...", 0.5))

    def test_try_explain_validity(self):
        # - test valid input
        self.assertIsNotNone(vector_utils.try_explain_validity(WKT_STR_EXAMPLE), "WKT should not be None.")
        self.assertIsNotNone(vector_utils.try_explain_validity(GEOJSON_STR_EXAMPLE), "Geojson should not be None.")
        self.assertIsNotNone(vector_utils.try_explain_validity(WKB_BIN_EXAMPLE), "WKB bytes should not be None.")
        self.assertIsNotNone(vector_utils.try_explain_validity(WKB_HEX_EXAMPLE), "WKB HEX should not be None.")

        # - test invalid input
        self.assertIsNotNone(vector_utils.try_explain_validity(INVALID_EXAMPLE), "INVALID GEOM should not be None.")
        self.assertIsNone(vector_utils.try_explain_validity(""), "Empty str input should return None.")
        self.assertIsNone(vector_utils.try_explain_validity("..."), "Non-geom str input should return None.")
        self.assertIsNone(vector_utils.try_explain_validity(None), "None input should return None.")

    def test_try_make_valid(self):
        # - test valid input that should not change
        self.assertEqual(
            shapely.to_wkt(vector_utils.try_make_valid(MULTI_POLY_EXAMPLE)),
            shapely.to_wkt(shapely.from_wkt(MULTI_POLY_EXAMPLE))
        )
        self.assertEqual(
            shapely.to_wkt(vector_utils.try_make_valid(WKT_STR_EXAMPLE)),
            shapely.to_wkt(shapely.from_wkt(WKT_STR_EXAMPLE))
        )
        self.assertEqual(
            shapely.to_wkt(vector_utils.try_make_valid(MULTI_LINE_EXAMPLE)),
            shapely.to_wkt(shapely.from_wkt(MULTI_LINE_EXAMPLE))
        )
        self.assertEqual(
            shapely.to_wkt(vector_utils.try_make_valid(LINE_EXAMPLE)),
            shapely.to_wkt(shapely.from_wkt(LINE_EXAMPLE))
        )
        self.assertEqual(
            shapely.to_wkt(vector_utils.try_make_valid(MULTI_POINT_EXAMPLE)),
            shapely.to_wkt(shapely.from_wkt(MULTI_POINT_EXAMPLE))
        )
        self.assertEqual(
            shapely.to_wkt(vector_utils.try_make_valid(POINT_EXAMPLE)),
            shapely.to_wkt(shapely.from_wkt(POINT_EXAMPLE))
        )

        # - test valid input that should change
        self.assertNotEqual(
            shapely.to_wkt(vector_utils.try_make_valid(INVALID_EXAMPLE)),
            shapely.to_wkt(shapely.from_wkt(INVALID_EXAMPLE))
        )

        # - test invalid input that should fail
        self.assertIsNone(vector_utils.try_make_valid(""))
        self.assertIsNone(vector_utils.try_make_valid("..."))
        self.assertIsNone(vector_utils.try_make_valid(None))

    def test_try_multi_flatten(self):
        # - test valid input (multi)
        self.assertTrue(len(vector_utils.try_multi_flatten(MULTI_POLY_EXAMPLE)) == 2, "Multipoly should flatten into 2 polys.")
        self.assertTrue(len(vector_utils.try_multi_flatten(MULTI_LINE_EXAMPLE)) == 2, "Multiline should flatten into 2 lines.")
        self.assertTrue(len(vector_utils.try_multi_flatten(MULTI_POINT_EXAMPLE)) == 2, "Multipoint should flatten into 2 points.")
        self.assertTrue(len(vector_utils.try_multi_flatten(COLLECTION_EXAMPLE)) == 4, "Collection (with Multipoint) should flatten into 4 geoms.")

        # - test valid input (not multi)
        self.assertTrue(len(vector_utils.try_multi_flatten(WKT_STR_EXAMPLE)) == 1, msg="WKT poly should return len 1.")
        self.assertTrue(len(vector_utils.try_multi_flatten(GEOJSON_STR_EXAMPLE)) == 1, msg="Geojson poly should return len 1.")
        self.assertTrue(len(vector_utils.try_multi_flatten(WKB_BIN_EXAMPLE)) == 1, msg="WKB bytes poly should return len 1.")
        self.assertTrue(len(vector_utils.try_multi_flatten(WKB_HEX_EXAMPLE)) == 1, msg="WKB HEX poly should return len 1.")

        # - test invalid input
        self.assertIsNone(vector_utils.try_multi_flatten(""), msg="Empty str input should return None.")
        self.assertIsNone(vector_utils.try_multi_flatten("..."), msg="Non-geom str input should return None.")
        self.assertIsNone(vector_utils.try_multi_flatten(None), msg="None input should return None.")

    def test_try_unary_union(self):
        # - test valid input (single geom)
        self.assertIsNotNone(vector_utils.try_unary_union(WKT_STR_EXAMPLE))
        self.assertIsNotNone(vector_utils.try_unary_union(MULTI_POLY_EXAMPLE))
        self.assertIsNotNone(vector_utils.try_unary_union(MULTI_LINE_EXAMPLE))
        self.assertIsNotNone(vector_utils.try_unary_union(MULTI_POINT_EXAMPLE))
        self.assertIsNotNone(vector_utils.try_unary_union(POINT_EXAMPLE))
        self.assertIsNotNone(vector_utils.try_unary_union(LINE_EXAMPLE))
        self.assertIsNotNone(vector_utils.try_unary_union(COLLECTION_EXAMPLE))

        # - test valid input (geom list)
        self.assertIsNotNone(vector_utils.try_unary_union([WKT_STR_EXAMPLE]))
        self.assertIsNotNone(vector_utils.try_unary_union([MULTI_POLY_EXAMPLE]))
        self.assertIsNotNone(vector_utils.try_unary_union([MULTI_LINE_EXAMPLE]))
        self.assertIsNotNone(vector_utils.try_unary_union([MULTI_POINT_EXAMPLE]))
        self.assertIsNotNone(vector_utils.try_unary_union([POINT_EXAMPLE]))
        self.assertIsNotNone(vector_utils.try_unary_union([LINE_EXAMPLE]))
        self.assertIsNotNone(vector_utils.try_unary_union([COLLECTION_EXAMPLE]))

        # - test valid input (all in geom list)
        self.assertIsNotNone(
            vector_utils.try_unary_union([
                WKT_STR_EXAMPLE,
                MULTI_POLY_EXAMPLE,
                MULTI_LINE_EXAMPLE,
                MULTI_POINT_EXAMPLE,
                POINT_EXAMPLE,
                LINE_EXAMPLE,
                COLLECTION_EXAMPLE
            ])
        )

        # - test invalid input
        self.assertIsNone(vector_utils.try_unary_union("..."))

    def test_try_update_srid(self):
        # - test valid input (valid updates)
        self.assertIsNotNone(vector_utils.try_update_srid(WKT_STR_EXAMPLE, 4326, 3857),"WKT should not be None.")
        self.assertIsNotNone(vector_utils.try_update_srid(GEOJSON_STR_EXAMPLE, 4326, 3857),"Geojson should not be None.")
        self.assertIsNotNone(vector_utils.try_update_srid(WKB_BIN_EXAMPLE, 4326, 3857),"WKB bytes should not be None.")
        self.assertIsNotNone(vector_utils.try_update_srid(WKB_HEX_EXAMPLE, 4326, 3857),"WKB HEX should not be None.")

        self.assertIsNotNone(vector_utils.try_update_srid(WKT_STR_EXAMPLE, 0, 4326),"WKT should not be None.")
        self.assertIsNotNone(vector_utils.try_update_srid(GEOJSON_STR_EXAMPLE, 0, 4326),"Geojson should not be None.")
        self.assertIsNotNone(vector_utils.try_update_srid(WKB_BIN_EXAMPLE, 0, 4326),"WKB bytes should not be None.")
        self.assertIsNotNone(vector_utils.try_update_srid(WKB_HEX_EXAMPLE, 0, 4326),"WKB HEX should not be None.")

        # - test invalid input
        self.assertIsNone(vector_utils.try_update_srid("", 4326, 3857),"Empty str input should return None.")
        self.assertIsNone(vector_utils.try_update_srid("...", 4326, 3857),"Non-geom str input should return None.")
        self.assertIsNone(vector_utils.try_update_srid(None, 4326, 3857),"None input should return None.")

        self.assertIsNone(vector_utils.try_update_srid(WKT_STR_EXAMPLE, 4326, 0),"WKT to_srid <= 0 should return None.")
        self.assertIsNone(vector_utils.try_update_srid(GEOJSON_STR_EXAMPLE, 4326, 0),"Geojson to_srid <= 0 should return None.")
        self.assertIsNone(vector_utils.try_update_srid(WKB_BIN_EXAMPLE, 4326, 0),"WKB to_srid <= 0 bytes should return None.")
        self.assertIsNone(vector_utils.try_update_srid(WKB_HEX_EXAMPLE, 4326, 0),"WKB HEX to_srid <= 0 should return None.")

if __name__ == '__main__':
    unittest.main()