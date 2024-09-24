from pyspark.sql.functions import abs, col, first, lit, sqrt, array, element_at

from .context import api, readers
from .utils import MosaicTestCaseWithGDAL


class TestRasterFunctions(MosaicTestCaseWithGDAL):
    def setUp(self) -> None:
        return super().setUp()

    def test_read_raster(self):
        result = self.generate_singleband_raster_df().first()
        self.assertEqual(result.length, 1067862)
        self.assertEqual(result.x_size, 2400)
        self.assertEqual(result.y_size, 2400)
        self.assertEqual(result.srid, 0)
        self.assertEqual(result.bandCount, 1)
        self.assertEqual(
            result.metadata["LONGNAME"],
            "MODIS/Terra+Aqua BRDF/Albedo Nadir BRDF-Adjusted Ref Daily L3 Global - 500m",
        )
        self.assertEqual(result.tile["metadata"]["driver"], "GTiff")

    def test_raster_scalar_functions(self):
        result = (
            self.generate_singleband_raster_df()
            .withColumn("rst_bandmetadata", api.rst_bandmetadata("tile", lit(1)))
            .withColumn("rst_boundingbox", api.rst_boundingbox("tile"))
            .withColumn(
                "rst_boundingbox", api.st_buffer("rst_boundingbox", lit(-0.001))
            )
            .withColumn("rst_clip", api.rst_clip("tile", "rst_boundingbox"))
            .withColumn(
                "rst_combineavg",
                api.rst_combineavg(array(col("tile"), col("rst_clip"))),
            )
            .withColumn("rst_frombands", api.rst_frombands(array("tile", "tile")))
            .withColumn("tile_from_file", api.rst_fromfile("path", lit(-1)))
            .withColumn("rst_georeference", api.rst_georeference("tile"))
            .withColumn("rst_getnodata", api.rst_getnodata("tile"))
            .withColumn("rst_subdatasets", api.rst_subdatasets("tile"))
            # .withColumn("rst_getsubdataset", api.rst_getsubdataset("tile"))
            .withColumn("rst_height", api.rst_height("tile"))
            .withColumn("rst_initnodata", api.rst_initnodata("tile"))
            .withColumn("rst_isempty", api.rst_isempty("tile"))
            .withColumn("rst_memsize", api.rst_memsize("tile"))
            .withColumn("rst_merge", api.rst_merge(array("tile", "tile")))
            .withColumn("rst_metadata", api.rst_metadata("tile"))
            .withColumn("rst_ndvi", api.rst_ndvi("tile", lit(1), lit(1)))
            .withColumn("rst_numbands", api.rst_numbands("tile"))
            .withColumn("rst_pixelheight", api.rst_pixelheight("tile"))
            .withColumn("rst_pixelwidth", api.rst_pixelwidth("tile"))
            .withColumn("rst_rastertogridavg", api.rst_rastertogridavg("tile", lit(9)))
            .withColumn(
                "rst_rastertogridcount", api.rst_rastertogridcount("tile", lit(9))
            )
            .withColumn("rst_rastertogridmax", api.rst_rastertogridmax("tile", lit(9)))
            .withColumn(
                "rst_rastertogridmedian", api.rst_rastertogridmedian("tile", lit(9))
            )
            .withColumn("rst_rastertogridmin", api.rst_rastertogridmin("tile", lit(9)))
            .withColumn(
                "rst_rastertoworldcoordx",
                api.rst_rastertoworldcoordx("tile", lit(1200), lit(1200)),
            )
            .withColumn(
                "rst_rastertoworldcoordy",
                api.rst_rastertoworldcoordy("tile", lit(1200), lit(1200)),
            )
            .withColumn(
                "rst_rastertoworldcoord",
                api.rst_rastertoworldcoord("tile", lit(1200), lit(1200)),
            )
            .withColumn("rst_rotation", api.rst_rotation("tile"))
            .withColumn("rst_scalex", api.rst_scalex("tile"))
            .withColumn("rst_scaley", api.rst_scaley("tile"))
            .withColumn("rst_srid", api.rst_srid("tile"))
            .withColumn("rst_summary", api.rst_summary("tile"))
            # .withColumn("rst_tryopen", api.rst_tryopen(col("path"))) # needs an issue
            .withColumn("rst_upperleftx", api.rst_upperleftx("tile"))
            .withColumn("rst_upperlefty", api.rst_upperlefty("tile"))
            .withColumn("rst_width", api.rst_width("tile"))
            .withColumn(
                "rst_worldtorastercoordx",
                api.rst_worldtorastercoordx("tile", lit(0.0), lit(0.0)),
            )
            .withColumn(
                "rst_worldtorastercoordy",
                api.rst_worldtorastercoordy("tile", lit(0.0), lit(0.0)),
            )
            .withColumn(
                "rst_worldtorastercoord",
                api.rst_worldtorastercoord("tile", lit(0.0), lit(0.0)),
            )
        )
        result.write.format("noop").mode("overwrite").save()
        self.assertEqual(result.count(), 1)

    def test_raster_flatmap_functions(self):
        retile_result = self.generate_singleband_raster_df().withColumn(
            "rst_retile", api.rst_retile("tile", lit(1200), lit(1200))
        )
        retile_result.write.format("noop").mode("overwrite").save()
        self.assertEqual(retile_result.count(), 4)

        subdivide_result = self.generate_singleband_raster_df().withColumn(
            "rst_subdivide", api.rst_subdivide("tile", lit(1))
        )
        subdivide_result.write.format("noop").mode("overwrite").save()
        self.assertEqual(retile_result.count(), 4)

        # TODO: reproject into WGS84
        tessellate_result = self.generate_singleband_raster_df().withColumn(
            "rst_tessellate", api.rst_tessellate("tile", lit(3))
        )

        tessellate_result.write.format("noop").mode("overwrite").save()
        self.assertEqual(tessellate_result.count(), 63)

        overlap_result = (
            self.generate_singleband_raster_df()
            .withColumn(
                "rst_to_overlapping_tiles",
                api.rst_to_overlapping_tiles("tile", lit(200), lit(200), lit(10)),
            )
            .withColumn("rst_subdatasets", api.rst_subdatasets("tile"))
        )

        overlap_result.write.format("noop").mode("overwrite").save()
        self.assertEqual(overlap_result.count(), 87)

    def test_raster_aggregator_functions(self):
        collection = (
            self.generate_singleband_raster_df()
            .withColumn("extent", api.st_astext(api.rst_boundingbox("tile")))
            .withColumn(
                "rst_to_overlapping_tiles",
                api.rst_to_overlapping_tiles("tile", lit(200), lit(200), lit(10)),
            )
        )

        merge_result = (
            collection.groupBy("path")
            .agg(api.rst_merge_agg("tile").alias("tile"))
            .withColumn("extent", api.st_astext(api.rst_boundingbox("tile")))
        )

        self.assertEqual(merge_result.count(), 1)
        self.assertEqual(
            collection.select("extent").first(), merge_result.select("extent").first()
        )

        combine_avg_result = (
            collection.groupBy("path")
            .agg(api.rst_combineavg_agg("tile").alias("tile"))
            .withColumn("extent", api.st_astext(api.rst_boundingbox("tile")))
        )

        self.assertEqual(combine_avg_result.count(), 1)
        self.assertEqual(
            collection.select("extent").first(),
            combine_avg_result.select("extent").first(),
        )

    def test_netcdf_load_tessellate_clip_merge(self):
        target_resolution = 1

        region_keys = ["NAME", "STATE", "BOROUGH", "BLOCK", "TRACT"]

        census_df = (
            readers.read()
            .format("multi_read_ogr")
            .option("vsizip", "true")
            .option("chunkSize", "20")
            .load("test/data/Blocks2020.zip")
            .select(*region_keys, "geom_0", "geom_0_srid")
            .dropDuplicates()
            .withColumn("geom_0", api.st_simplify("geom_0", lit(0.001)))
            .withColumn(
                "geom_0", api.st_updatesrid("geom_0", col("geom_0_srid"), lit(4326))
            )
            .withColumn(
                "chip", api.grid_tessellateexplode("geom_0", lit(target_resolution))
            )
            .select(*region_keys, "chip.*")
        )

        df = (
            self.spark.read.format("gdal")
            .option("raster.read.strategy", "in_memory")
            .load(
                "test/data/prAdjust_day_HadGEM2-CC_SMHI-DBSrev930-GFD-1981-2010-postproc_rcp45_r1i1p1_20201201-20201231.nc"
            )
            .select(api.rst_separatebands("tile").alias("tile"))
            .repartition(self.spark.sparkContext.defaultParallelism)
            .withColumn(
                "timestep",
                element_at(
                    api.rst_metadata("tile"), "NC_GLOBAL#GDAL_MOSAIC_BAND_INDEX"
                ),
            )
            .withColumn("tile", api.rst_setsrid("tile", lit(4326)))
            .where(col("timestep") == 21)
            .withColumn(
                "tile", api.rst_to_overlapping_tiles("tile", lit(20), lit(20), lit(10))
            )
            .repartition(self.spark.sparkContext.defaultParallelism)
        )

        prh_bands_indexed = df.withColumn(
            "tile", api.rst_tessellate("tile", lit(target_resolution))
        )

        clipped_precipitation = (
            prh_bands_indexed.alias("var")
            .join(
                census_df.alias("aoi"),
                how="inner",
                on=col("var.tile.index_id") == col("aoi.index_id"),
            )
            .withColumn("tile", api.rst_clip("var.tile", "aoi.wkb"))
        )

        merged_precipitation = clipped_precipitation.groupBy(*region_keys).agg(
            api.rst_merge_agg("tile").alias("tile")
        )

        self.assertEqual(merged_precipitation.count(), 1)
