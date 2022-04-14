from pyspark.sql.functions import col, lit

from .context import api
from .utils import MosaicTestCase


class TestFunctions(MosaicTestCase):
    def test_st_point(self):
        expected = [
            "POINT (0 0)",
            "POINT (1 1)",
            "POINT (2 2)",
            "POINT (3 3)",
            "POINT (4 4)",
        ]
        result = (
            self.spark.range(5)
            .select(col("id").cast("double"))
            .withColumn("points", api.st_point("id", "id"))
            .withColumn("points", api.st_astext("points"))
            .collect()
        )
        self.assertListEqual([rw.points for rw in result], expected)

    def test_st_bindings_happy_flow(self):
        # Checks that the python bindings do not throw exceptions
        # Not testing the logic, since that is tested in Scala
        df = self.spark.createDataFrame(
            [
                # 2x1 rectangle starting at (0 0)
                ["POLYGON ((0 0, 0 2, 1 2, 1 0, 0 0))", "POINT (1 1)"]
            ],
            ["wkt", "point_wkt"],
        )

        result = (
            df.withColumn("st_area", api.st_area("wkt"))
            .withColumn("st_length", api.st_length("wkt"))
            .withColumn("st_perimeter", api.st_perimeter("wkt"))
            .withColumn("st_convexhull", api.st_convexhull("wkt"))
            .withColumn("st_dump", api.st_dump("wkt"))
            .withColumn("st_translate", api.st_translate("wkt", lit(1), lit(1)))
            .withColumn("st_scale", api.st_scale("wkt", lit(1), lit(1)))
            .withColumn("st_rotate", api.st_rotate("wkt", lit(1)))
            .withColumn("st_centroid2D", api.st_centroid2D("wkt"))
            .withColumn("st_centroid3D", api.st_centroid3D("wkt"))
            .withColumn("st_length", api.st_length("wkt"))
            .withColumn("st_isvalid", api.st_isvalid("wkt"))
            .withColumn("st_intersects", api.st_intersects("wkt", "wkt"))
            .withColumn("st_intersection", api.st_intersection("wkt", "wkt"))
            .withColumn("st_geometrytype", api.st_geometrytype("wkt"))
            .withColumn("st_isvalid", api.st_isvalid("wkt"))
            .withColumn("st_xmin", api.st_xmin("wkt"))
            .withColumn("st_xmax", api.st_xmax("wkt"))
            .withColumn("st_ymin", api.st_ymin("wkt"))
            .withColumn("st_ymax", api.st_ymax("wkt"))
            .withColumn("st_zmin", api.st_zmin("wkt"))
            .withColumn("st_zmax", api.st_zmax("wkt"))
            .withColumn("flatten_polygons", api.flatten_polygons("wkt"))
            .withColumn("point_index_lonlat", api.point_index_lonlat(lit(1), lit(1), lit(1)))
            .withColumn("point_index_geom", api.point_index_geom("point_wkt", lit(1)))
            .withColumn("index_geometry", api.index_geometry(lit(1)))
            .withColumn("polyfill", api.polyfill("wkt", lit(1)))
            .withColumn("mosaic_explode", api.mosaic_explode("wkt", lit(1)))
            .withColumn("mosaicfill", api.mosaicfill("wkt", lit(1)))
            .withColumn("mosaic_explode_no_core_chips", api.mosaic_explode("wkt", lit(1), False))
            .withColumn("mosaicfill_no_core_chips", api.mosaicfill("wkt", lit(1), False))
        )

        self.assertEqual(result.count(), 1)
