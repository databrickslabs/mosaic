generate_singleband_raster_df <- function() {
  read.df(
    path = "sparkrMosaic/tests/testthat/data/MCD43A4.A2018185.h10v07.006.2018194033728_B04.TIF",
    source = "gdal",
    raster.read.strategy = "in_memory"
    )
}

test_that("mosaic can read single-band GeoTiff", {
  sdf <- generate_singleband_raster_df()

  row <- first(sdf)
  expect_equal(row$length, 1067862L)
  expect_equal(row$x_size, 2400)
  expect_equal(row$y_size, 2400)
  expect_equal(row$srid, 0)
  expect_equal(row$bandCount, 1)
  expect_equal(row$metadata[[1]]$LONGNAME, "MODIS/Terra+Aqua BRDF/Albedo Nadir BRDF-Adjusted Ref Daily L3 Global - 500m")
  expect_equal(row$tile[[1]]$metadata$driver, "GTiff")

})

test_that("scalar raster functions behave as intended", {
  sdf <- generate_singleband_raster_df()
  sdf <- withColumn(sdf, "rst_rastertogridavg", rst_rastertogridavg(column("tile"), lit(9L)))
  sdf <- withColumn(sdf, "rst_rastertogridcount", rst_rastertogridcount(column("tile"), lit(9L)))
  sdf <- withColumn(sdf, "rst_rastertogridmax", rst_rastertogridmax(column("tile"), lit(9L)))
  sdf <- withColumn(sdf, "rst_rastertogridmedian", rst_rastertogridmedian(column("tile"), lit(9L)))
  sdf <- withColumn(sdf, "rst_rastertogridmin", rst_rastertogridmin(column("tile"), lit(9L)))
  sdf <- withColumn(sdf, "rst_rastertoworldcoordx", rst_rastertoworldcoordx(column("tile"), lit(1200L), lit(1200L)))
  sdf <- withColumn(sdf, "rst_rastertoworldcoordy", rst_rastertoworldcoordy(column("tile"), lit(1200L), lit(1200L)))
  sdf <- withColumn(sdf, "rst_rastertoworldcoord", rst_rastertoworldcoord(column("tile"), lit(1200L), lit(1200L)))
  sdf <- withColumn(sdf, "rst_rotation", rst_rotation(column("tile")))
  sdf <- withColumn(sdf, "rst_scalex", rst_scalex(column("tile")))
  sdf <- withColumn(sdf, "rst_scaley", rst_scaley(column("tile")))
  sdf <- withColumn(sdf, "rst_srid", rst_srid(column("tile")))
  sdf <- withColumn(sdf, "rst_summary", rst_summary(column("tile")))
  sdf <- withColumn(sdf, "rst_type", rst_type(column("tile")))
  sdf <- withColumn(sdf, "rst_updatetype", rst_updatetype(column("tile"), lit("Float32")))
  sdf <- withColumn(sdf, "rst_upperleftx", rst_upperleftx(column("tile")))
  sdf <- withColumn(sdf, "rst_upperlefty", rst_upperlefty(column("tile")))
  sdf <- withColumn(sdf, "rst_width", rst_width(column("tile")))
  sdf <- withColumn(sdf, "rst_worldtorastercoordx", rst_worldtorastercoordx(column("tile"), lit(0.0), lit(0.0)))
  sdf <- withColumn(sdf, "rst_worldtorastercoordy", rst_worldtorastercoordy(column("tile"), lit(0.0), lit(0.0)))
  sdf <- withColumn(sdf, "rst_worldtorastercoord", rst_worldtorastercoord(column("tile"), lit(0.0), lit(0.0)))
  sdf <- withColumn(sdf, "rst_write", rst_write(column("tile"), lit("/mnt/mosaic_tmp/write-raster")))

  expect_no_error(write.df(sdf, source = "noop", mode = "overwrite"))
})

test_that("raster flatmap functions behave as intended", {
  retiled_sdf <- generate_singleband_raster_df()
  retiled_sdf <- withColumn(retiled_sdf, "rst_retile", rst_retile(column("tile"), lit(1200L), lit(1200L)))

  expect_no_error(write.df(retiled_sdf, source = "noop", mode = "overwrite"))
  expect_equal(nrow(retiled_sdf), 4)

  subdivide_sdf <- generate_singleband_raster_df()
  subdivide_sdf <- withColumn(subdivide_sdf, "rst_subdivide", rst_subdivide(column("tile"), lit(1L)))

  expect_no_error(write.df(subdivide_sdf, source = "noop", mode = "overwrite"))
  expect_equal(nrow(subdivide_sdf), 4)

  tessellate_sdf <- generate_singleband_raster_df()
  tessellate_sdf <- withColumn(tessellate_sdf, "rst_tessellate", rst_tessellate(column("tile"), lit(3L)))

  expect_no_error(write.df(tessellate_sdf, source = "noop", mode = "overwrite"))
  expect_equal(nrow(tessellate_sdf), 63)

  overlap_sdf <- generate_singleband_raster_df()
  overlap_sdf <- withColumn(overlap_sdf, "rst_tooverlappingtiles", rst_tooverlappingtiles(column("tile"), lit(200L), lit(200L), lit(10L)))

  expect_no_error(write.df(overlap_sdf, source = "noop", mode = "overwrite"))
  expect_equal(nrow(overlap_sdf), 87)
})

test_that("raster aggregation functions behave as intended", {
  collection_sdf <- generate_singleband_raster_df()
  collection_sdf <- withColumn(collection_sdf, "extent", st_astext(rst_boundingbox(column("tile"))))
  collection_sdf <- withColumn(collection_sdf, "tile", rst_tooverlappingtiles(column("tile"), lit(200L), lit(200L), lit(10L)))

  merge_sdf <- summarize(
    groupBy(collection_sdf, "path"),
    alias(rst_merge_agg(column("tile")), "tile")
    )
  merge_sdf <- withColumn(merge_sdf, "extent", st_astext(rst_boundingbox(column("tile"))))

  expect_equal(nrow(merge_sdf), 1)
  expect_equal(first(collection_sdf)$extent, first(merge_sdf)$extent)

  combine_avg_sdf <- summarize(
    groupBy(collection_sdf, "path"),
    alias(rst_combineavg_agg(column("tile")), "tile")
  )
  combine_avg_sdf <- withColumn(combine_avg_sdf, "extent", st_astext(rst_boundingbox(column("tile"))))

  expect_equal(nrow(combine_avg_sdf), 1)
  expect_equal(first(collection_sdf)$extent, first(combine_avg_sdf)$extent)

})

test_that("the tessellate-join-clip-merge flow works on NetCDF files", {
  target_resolution <- 1L

  region_keys <- c("NAME", "STATE", "BOROUGH", "BLOCK", "TRACT")

  census_sdf <- read.df(
    path = "sparkrMosaic/tests/testthat/data/Blocks2020.zip",
    source = "com.databricks.labs.mosaic.datasource.OGRFileFormat",
    vsizip = "true",
    chunkSize = "20"
  )

  census_sdf <- select(census_sdf, c(region_keys, "geom_0", "geom_0_srid"))
  census_sdf <- distinct(census_sdf)
  census_sdf <- withColumn(census_sdf, "geom_0", st_simplify(column("geom_0"), lit(0.001)))
  census_sdf <- withColumn(census_sdf, "geom_0", st_updatesrid(column("geom_0"), column("geom_0_srid"), lit(4326L)))
  census_sdf <- withColumn(census_sdf, "chip", grid_tessellateexplode(column("geom_0"), lit(target_resolution)))
  census_sdf <- select(census_sdf, c(region_keys, "chip.*"))

  raster_sdf <- read.df(
    path = "sparkrMosaic/tests/testthat/data/prAdjust_day_HadGEM2-CC_SMHI-DBSrev930-GFD-1981-2010-postproc_rcp45_r1i1p1_20201201-20201231.nc",
    source = "gdal",
    raster.read.strategy = "in_memory"
  )

  raster_sdf <- withColumn(raster_sdf, "tile", rst_separatebands(column("tile")))
  raster_sdf <- withColumn(raster_sdf, "timestep", element_at(rst_metadata(column("tile")), "NC_GLOBAL#GDAL_MOSAIC_BAND_INDEX"))
  raster_sdf <- where(raster_sdf, "timestep = 21")
  raster_sdf <- withColumn(raster_sdf, "tile", rst_setsrid(column("tile"), lit(4326L)))
  raster_sdf <- withColumn(raster_sdf, "tile", rst_tooverlappingtiles(column("tile"), lit(20L), lit(20L), lit(10L)))
  raster_sdf <- withColumn(raster_sdf, "tile", rst_tessellate(column("tile"), lit(target_resolution)))

  clipped_sdf <- join(raster_sdf, census_sdf, raster_sdf$tile.index_id == census_sdf$index_id)
  clipped_sdf <- withColumn(clipped_sdf, "tile", rst_clip(column("tile"), column("wkb")))

  merged_precipitation <- summarize(
    groupBy(clipped_sdf, "timestep"),
    alias(rst_merge_agg(column("tile")), "tile")
  )

  expect_equal(nrow(merged_precipitation), 1)

})

test_that("a terrain model can be produced from point geometries", {

sdf <- createDataFrame(
  data.frame(
    wkt = c(
      "POINT Z (3 2 1)",
      "POINT Z (2 1 0)",
      "POINT Z (1 3 3)",
      "POINT Z (0 2 2)"
    )
  )
)

sdf <- agg(groupBy(sdf), masspoints = collect_list(column("wkt")))
sdf <- withColumn(sdf, "breaklines", expr("array('LINESTRING EMPTY')"))
sdf <- withColumn(sdf, "splitPointFinder", lit("NONENCROACHING"))
sdf <- withColumn(sdf, "origin", st_geomfromwkt(lit("POINT (0.6 1.8)")))
sdf <- withColumn(sdf, "xWidth", lit(12L))
sdf <- withColumn(sdf, "yWidth", lit(6L))
sdf <- withColumn(sdf, "xSize", lit(0.1))
sdf <- withColumn(sdf, "ySize", lit(0.1))
sdf <- withColumn(sdf, "noData", lit(-9999.0))
sdf <- withColumn(sdf, "tile", rst_dtmfromgeoms(
column("masspoints"), column("breaklines"), lit(0.0), lit(0.01), column("splitPointFinder"),
column("origin"), column("xWidth"), column("yWidth"), column("xSize"), column("ySize"), column("noData")
))
expect_equal(SparkR::count(sdf), 1)
})