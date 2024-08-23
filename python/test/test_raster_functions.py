from pyspark.sql import DataFrame
from pyspark.sql.functions import abs, array, col, element_at, first, lit, sqrt

from .context import api, readers
from .utils import MosaicTestCaseWithGDAL


class TestRasterFunctions(MosaicTestCaseWithGDAL):
    def setUp(self) -> None:
        return super().setUp()

    def test_read_raster(self):
        """
        Uses the non-transformed singleband raster.
        """
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
        """
        Uses the 4326 transformed singleband raster.
        """
        result = (
            self.generate_singleband_4326_raster_df()
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
            .withColumn("rst_avg", api.rst_avg("tile"))
            .withColumn("rst_max", api.rst_max("tile"))
            .withColumn("rst_median", api.rst_median("tile"))
            .withColumn("rst_min", api.rst_min("tile"))
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
            .withColumn("rst_pixelcount", api.rst_pixelcount("tile"))
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
            .cache()
        )
        result_cnt = result.count()
        print(f"result - count? {result_cnt}")
        self.assertEqual(result_cnt, 1)
        # result.limit(1).show() # <- too messy (skipping)
        result.unpersist()

    def test_raster_flatmap_functions(self):
        """
        Uses the 4326 transformed singleband raster.
        """
        retile_result = (
            self.generate_singleband_4326_raster_df()
            .withColumn("rst_retile", api.rst_retile("tile", lit(1200), lit(1200)))
            .cache()
        )
        retile_cnt = retile_result.count()
        print(f"retile - count? {retile_cnt}")
        self.assertEqual(retile_cnt, 2)
        retile_result.limit(1).show()
        retile_result.unpersist()

        subdivide_result = (
            self.generate_singleband_4326_raster_df()
            .withColumn("rst_subdivide", api.rst_subdivide("tile", lit(1)))
            .cache()
        )
        subdivide_cnt = subdivide_result.count()
        print(f"subdivide - count? {subdivide_cnt}")
        self.assertEqual(subdivide_cnt, 13)
        subdivide_result.limit(1).show()
        subdivide_result.unpersist()

        tessellate_result = (
            self.generate_singleband_4326_raster_df()
            .withColumn("srid", api.rst_srid("tile"))
            .withColumn("rst_tessellate", api.rst_tessellate("tile", lit(3)))
            .cache()
        )
        tessellate_cnt = tessellate_result.count()
        print(
            f"tessellate - count? {tessellate_cnt} (srid? {tessellate_result.select('srid').first()[0]})"
        )
        self.assertEqual(tessellate_cnt, 63)
        tessellate_result.limit(1).show()
        tessellate_result.unpersist()

        overlap_result = (
            self.generate_singleband_4326_raster_df()
            .withColumn(
                "rst_tooverlappingtiles",
                api.rst_tooverlappingtiles("tile", lit(200), lit(200), lit(10)),
            )
            .withColumn("rst_subdatasets", api.rst_subdatasets("tile"))
            .cache()
        )
        overlap_cnt = overlap_result.count()
        print(f"overlap - count? {overlap_cnt}")
        self.assertEqual(overlap_cnt, 67)
        overlap_result.limit(1).show()
        overlap_result.unpersist()

    def test_raster_aggregator_functions(self):
        """
        Uses the non-transformed singleband raster.
        """
        collection = (
            self.generate_singleband_raster_df()
            .withColumn("extent", api.st_astext(api.rst_boundingbox("tile")))
            .withColumn(
                "tile",
                api.rst_tooverlappingtiles("tile", lit(200), lit(200), lit(10)),
            )
            .cache()
        )
        collection_cnt = collection.count()
        print(f"collection - count? {collection_cnt}")  # <- 87
        collection.limit(1).show()

        merge_result = (
            collection.groupBy("path")
            .agg(api.rst_merge_agg("tile").alias("merge_tile"))
            .withColumn("extent", api.st_astext(api.rst_boundingbox("merge_tile")))
            .cache()
        )
        merge_cnt = merge_result.count()
        print(f"merge agg - count? {merge_cnt}")
        merge_result.limit(1).show()

        self.assertEqual(merge_cnt, 1)
        self.assertEqual(
            collection.select("extent").first(), merge_result.select("extent").first()
        )

        combine_avg_result = (
            collection.groupBy("path")
            .agg(api.rst_combineavg_agg("tile").alias("tile"))
            .withColumn("extent", api.st_astext(api.rst_boundingbox("tile")))
            .cache()
        )
        combine_cnt = combine_avg_result.count()
        print(f"combine avg - count? {combine_cnt}")
        combine_avg_result.limit(1).show()

        self.assertEqual(combine_cnt, 1)
        self.assertEqual(
            collection.select("extent").first(),
            combine_avg_result.select("extent").first(),
        )
        combine_avg_result.unpersist()

    def test_netcdf_load_tessellate_clip_merge(self):
        target_resolution = 1

        region_keys = ["NAME", "STATE", "BOROUGH", "BLOCK", "TRACT"]

        census_df: DataFrame = None
        df: DataFrame = None
        prh_bands_indexed: DataFrame = None
        clipped_precipitation: DataFrame = None
        merged_precipitation: DataFrame = None

        try:
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
                .cache()
            )
            census_df_cnt = census_df.count()
            print(f"...census_df count? {census_df_cnt}")
            self.assertEqual(census_df_cnt, 2)
            census_df.limit(1).show()

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
                    "tile",
                    api.rst_tooverlappingtiles("tile", lit(20), lit(20), lit(10)),
                )
                .repartition(self.spark.sparkContext.defaultParallelism)
                .cache()
            )
            df_cnt = df.count()
            print(f"...df count? {df_cnt}")
            # print(f"...df tile? {df.select('tile').first()[0]}")
            # print(f"""... metadata -> {df.select(api.rst_metadata("tile")).first()[0]}""")
            # print(f"""... timesteps -> {[r[0] for r in df.select("timestep").distinct().collect()]}""")
            df.limit(1).show()

            prh_bands_indexed = df.withColumn(
                "tile", api.rst_tessellate("tile", lit(target_resolution))
            ).cache()
            prh_cnt = prh_bands_indexed.count()
            print(f"...prh count? {prh_cnt}")
            prh_bands_indexed.limit(1).show()

            clipped_precipitation = (
                prh_bands_indexed.alias("var")
                .join(
                    census_df.alias("aoi"),
                    how="inner",
                    on=col("var.tile.index_id") == col("aoi.index_id"),
                )
                .withColumn("tile", api.rst_clip("var.tile", "aoi.wkb"))
                .cache()
            )
            clipped_precip_cnt = clipped_precipitation.count()
            print(f"...clipped precip count? {clipped_precip_cnt}")
            clipped_precipitation.limit(1).show()

            merged_precipitation = (
                clipped_precipitation.groupBy(*region_keys)
                .agg(api.rst_merge_agg("tile").alias("tile"))
                .cache()
            )
            merged_precip_cnt = merged_precipitation.count()
            print(f"...merged precip count? {merged_precip_cnt}")
            self.assertEqual(merged_precip_cnt, 1)
            merged_precipitation.limit(1).show()

        finally:
            exec("try:census_df.unpersist() \nexcept:pass")
            exec("try:df.unpersist() \nexcept:pass")
            exec("try:prh_bands_indexed.unpersist() \nexcept:pass")
            exec("try:clipped_precipitation.unpersist() \nexcept:pass")
            exec("try:merged_precipitation.unpersist() \nexcept:pass")
