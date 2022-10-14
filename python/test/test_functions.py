import random

from pyspark.sql.functions import abs, col, first, lit, sqrt

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
            .withColumn("st_buffer", api.st_buffer("wkt", lit(1.1)))
            .withColumn("st_perimeter", api.st_perimeter("wkt"))
            .withColumn("st_convexhull", api.st_convexhull("wkt"))
            .withColumn("st_dump", api.st_dump("wkt"))
            .withColumn("st_translate", api.st_translate("wkt", lit(1), lit(1)))
            .withColumn("st_scale", api.st_scale("wkt", lit(1), lit(1)))
            .withColumn("st_rotate", api.st_rotate("wkt", lit(1)))
            .withColumn("st_centroid2D", api.st_centroid2D("wkt"))
            .withColumn("st_centroid3D", api.st_centroid3D("wkt"))
            .withColumn("st_numpoints", api.st_numpoints("wkt"))
            .withColumn("st_length", api.st_length("wkt"))
            .withColumn("st_isvalid", api.st_isvalid("wkt"))
            .withColumn(
                "st_hasvalidcoordinates",
                api.st_hasvalidcoordinates("wkt", lit("EPSG:2192"), lit("bounds")),
            )
            .withColumn("st_intersects", api.st_intersects("wkt", "wkt"))
            .withColumn("st_intersection", api.st_intersection("wkt", "wkt"))
            .withColumn("st_unaryunion", api.st_unaryunion("wkt"))
            .withColumn("st_geometrytype", api.st_geometrytype("wkt"))
            .withColumn("st_xmin", api.st_xmin("wkt"))
            .withColumn("st_xmax", api.st_xmax("wkt"))
            .withColumn("st_ymin", api.st_ymin("wkt"))
            .withColumn("st_ymax", api.st_ymax("wkt"))
            .withColumn("st_zmin", api.st_zmin("wkt"))
            .withColumn("st_zmax", api.st_zmax("wkt"))
            .withColumn("flatten_polygons", api.flatten_polygons("wkt"))

            # SRID functions
            .withColumn(
                "geom_with_srid", api.st_setsrid(api.st_geomfromwkt("wkt"), lit(4326))
            )
            .withColumn("srid_check", api.st_srid("geom_with_srid"))
            .withColumn(
                "transformed_geom", api.st_transform("geom_with_srid", lit(3857))
            )

            # Grid functions
            .withColumn("grid_longlatascellid", api.grid_longlatascellid(lit(1), lit(1), lit(1))
            )
            .withColumn("grid_pointascellid", api.grid_pointascellid("point_wkt", lit(1)))
            .withColumn("grid_boundaryaswkb", api.grid_boundaryaswkb(lit(1)))
            .withColumn("grid_polyfill", api.grid_polyfill("wkt", lit(1)))
            .withColumn("grid_tessellateexplode", api.grid_tessellateexplode("wkt", lit(1)))
            .withColumn(
                "grid_tessellateexplode_no_core_chips",
                api.grid_tessellateexplode("wkt", lit(1), lit(False)),
            )
            .withColumn(
                "grid_tessellateexplode_no_core_chips_bool",
                api.grid_tessellateexplode("wkt", lit(1), False),
            )
            .withColumn("grid_tessellate", api.grid_tessellate("wkt", lit(1)))


            # Deprecated
            .withColumn(
                "point_index_lonlat", api.point_index_lonlat(lit(1), lit(1), lit(1))
            )
            .withColumn("point_index_geom", api.point_index_geom("point_wkt", lit(1)))
            .withColumn("index_geometry", api.index_geometry(lit(1)))
            .withColumn("polyfill", api.polyfill("wkt", lit(1)))
            .withColumn("mosaic_explode", api.mosaic_explode("wkt", lit(1)))
            .withColumn(
                "mosaic_explode_no_core_chips",
                api.mosaic_explode("wkt", lit(1), lit(False)),
            )
            .withColumn(
                "mosaic_explode_no_core_chips_bool",
                api.mosaic_explode("wkt", lit(1), False),
            )
            .withColumn("mosaicfill", api.mosaicfill("wkt", lit(1)))
            .withColumn(
                "mosaicfill_no_core_chips", api.mosaicfill("wkt", lit(1), False)
            )
            .withColumn(
                "mosaicfill_no_core_chips_bool",
                api.mosaicfill("wkt", lit(1), lit(False)),
            )

        )

        self.assertEqual(result.count(), 1)

    def test_aggregation_functions(self):
        left_df = (
            self.generate_input_polygon_collection()
            .limit(1)
            .select(
                col("location_id").alias("left_id"),
                api.mosaic_explode(col("geometry"), lit(11)).alias("left_index"),
                col("geometry").alias("left_geom"),
            )
        )

        right_df = (
            self.generate_input_polygon_collection()
            .limit(1)
            .select(
                col("location_id"),
                api.st_translate(
                    col("geometry"),
                    sqrt(api.st_area(col("geometry")) * random.random() * 0.1),
                    sqrt(api.st_area(col("geometry")) * random.random() * 0.1),
                ).alias("geometry"),
            )
            .select(
                col("location_id").alias("right_id"),
                api.mosaic_explode(col("geometry"), lit(11)).alias("right_index"),
                col("geometry").alias("right_geom"),
            )
        )

        intersections_result = (
            left_df.drop("wkt")
            .join(right_df, col("left_index.index_id") == col("right_index.index_id"))
            .groupBy("left_id", "right_id")
            .agg(
                api.st_intersects_aggregate(
                    col("left_index"), col("right_index")
                ).alias("agg_intersects"),
                api.st_intersection_aggregate(
                    col("left_index"), col("right_index")
                ).alias("agg_intersection"),
                first("left_geom").alias("left_geom"),
                first("right_geom").alias("right_geom"),
            )
            .withColumn(
                "flat_intersects",
                api.st_intersects(col("left_geom"), col("right_geom")),
            )
            .withColumn(
                "comparison_intersects", col("agg_intersects") == col("flat_intersects")
            )
            .withColumn("agg_area", api.st_area(col("agg_intersection")))
            .withColumn(
                "flat_intersection",
                api.st_intersection(col("left_geom"), col("right_geom")),
            )
            .withColumn("flat_area", api.st_area(col("flat_intersection")))
            .withColumn(
                "comparison_intersection",
                abs(col("agg_area") - col("flat_area")) <= lit(1e-8),
            )  # ESRI Spatial tolerance
        )

        self.assertTrue(
            intersections_result.select("comparison_intersects").collect()[0][
                "comparison_intersects"
            ]
        )
        self.assertTrue(
            intersections_result.select("comparison_intersection").collect()[0][
                "comparison_intersection"
            ]
        )
