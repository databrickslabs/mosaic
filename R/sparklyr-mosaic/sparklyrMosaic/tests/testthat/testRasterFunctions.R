generate_singleband_raster_df <- function() {
  spark_read_source(
    sc,
    name = "raster",
    source = "gdal",
    path = "data/MCD43A4.A2018185.h10v07.006.2018194033728_B04.TIF",
    options = list("raster.read.strategy" = "in_memory")
  )
}


test_that("mosaic can read single-band GeoTiff", {
  sdf <- generate_singleband_raster_df()
  row <- sdf %>% head(1) %>% sdf_collect
  expect_equal(row$length, 1067862L)
  expect_equal(row$x_size, 2400)
  expect_equal(row$y_size, 2400)
  expect_equal(row$srid, 0)
  expect_equal(row$bandCount, 1)
  expect_equal(row$metadata[[1]]$LONGNAME, "MODIS/Terra+Aqua BRDF/Albedo Nadir BRDF-Adjusted Ref Daily L3 Global - 500m")
  expect_equal(row$tile[[1]]$driver, "GTiff")

})


test_that("scalar raster functions behave as intended", {
  sdf <- generate_singleband_raster_df() %>%
    mutate(rst_bandmetadata = rst_bandmetadata(tile, 1L)) %>%
    mutate(rst_boundingbox = rst_boundingbox(tile)) %>%
    mutate(rst_boundingbox = st_buffer(rst_boundingbox, -0.001)) %>%
    mutate(rst_clip = rst_clip(tile, rst_boundingbox)) %>%
    mutate(rst_combineavg = rst_combineavg(array(tile, rst_clip))) %>%
    mutate(rst_frombands = rst_frombands(array(tile, tile))) %>%
    mutate(rst_fromfile = rst_fromfile(path, -1L)) %>%
    mutate(rst_georeference = rst_georeference(tile)) %>%
    mutate(rst_getnodata = rst_getnodata(tile)) %>%
    mutate(rst_subdatasets = rst_subdatasets(tile)) %>%
    mutate(rst_height = rst_height(tile)) %>%
    mutate(rst_initnodata = rst_initnodata(tile)) %>%
    mutate(rst_isempty = rst_isempty(tile)) %>%
    mutate(rst_memsize = rst_memsize(tile)) %>%
    mutate(rst_merge = rst_merge(array(tile, tile))) %>%
    mutate(rst_metadata = rst_metadata(tile)) %>%
    mutate(rst_ndvi = rst_ndvi(tile, 1L, 1L)) %>%
    mutate(rst_numbands = rst_numbands(tile)) %>%
    mutate(rst_pixelheight = rst_pixelheight(tile)) %>%
    mutate(rst_pixelwidth = rst_pixelwidth(tile))

  # breaking the chain here to avoid memory issues
  expect_no_error(spark_write_source(sdf, "noop", mode = "overwrite"))

  sdf <- generate_singleband_raster_df() %>%
    mutate(rst_rastertogridavg = rst_rastertogridavg(tile, 9L)) %>%
    mutate(rst_rastertogridcount = rst_rastertogridcount(tile, 9L)) %>%
    mutate(rst_rastertogridmax = rst_rastertogridmax(tile, 9L)) %>%
    mutate(rst_rastertogridmedian = rst_rastertogridmedian(tile, 9L)) %>%
    mutate(rst_rastertogridmin = rst_rastertogridmin(tile, 9L)) %>%
    mutate(rst_rastertoworldcoordx = rst_rastertoworldcoordx(tile, 1200L, 1200L)) %>%
    mutate(rst_rastertoworldcoordy = rst_rastertoworldcoordy(tile, 1200L, 1200L)) %>%
    mutate(rst_rastertoworldcoord = rst_rastertoworldcoord(tile, 1200L, 1200L)) %>%
    mutate(rst_rotation = rst_rotation(tile)) %>%
    mutate(rst_scalex = rst_scalex(tile)) %>%
    mutate(rst_scaley = rst_scaley(tile)) %>%
    mutate(rst_srid = rst_srid(tile)) %>%
    mutate(rst_summary = rst_summary(tile)) %>%
    mutate(rst_upperleftx = rst_upperleftx(tile)) %>%
    mutate(rst_upperlefty = rst_upperlefty(tile)) %>%
    mutate(rst_width = rst_width(tile)) %>%
    mutate(rst_worldtorastercoordx = rst_worldtorastercoordx(tile, as.double(0.0), as.double(0.0))) %>%
    mutate(rst_worldtorastercoordy = rst_worldtorastercoordy(tile, as.double(0.0), as.double(0.0))) %>%
    mutate(rst_worldtorastercoord = rst_worldtorastercoord(tile, as.double(0.0), as.double(0.0)))

  expect_no_error(spark_write_source(sdf, "noop", mode = "overwrite"))
})

test_that("raster flatmap functions behave as intended", {
  retiled_sdf <- generate_singleband_raster_df() %>%
    mutate(rst_retile = rst_retile(tile, 1200L, 1200L))

  expect_no_error(spark_write_source(retiled_sdf, "noop", mode = "overwrite"))
  expect_equal(sdf_nrow(retiled_sdf), 4)

  subdivide_sdf <- generate_singleband_raster_df() %>%
    mutate(rst_subdivide = rst_subdivide(tile, 1L))

  expect_no_error(spark_write_source(subdivide_sdf, "noop", mode = "overwrite"))
  expect_equal(sdf_nrow(subdivide_sdf), 4)

  tessellate_sdf <- generate_singleband_raster_df() %>%
    mutate(rst_tessellate = rst_tessellate(tile, 3L))

  expect_no_error(spark_write_source(tessellate_sdf, "noop", mode = "overwrite"))
  expect_equal(sdf_nrow(tessellate_sdf), 66)

  overlap_sdf <- generate_singleband_raster_df() %>%
    mutate(rst_to_overlapping_tiles = rst_to_overlapping_tiles(tile, 200L, 200L, 10L))

  expect_no_error(spark_write_source(overlap_sdf, "noop", mode = "overwrite"))
  expect_equal(sdf_nrow(overlap_sdf), 87)

})

test_that("raster aggregation functions behave as intended", {
  collection_sdf <- generate_singleband_raster_df() %>%
    mutate(extent = st_astext(rst_boundingbox(tile))) %>%
    mutate(tile = rst_to_overlapping_tiles(tile, 200L, 200L, 10L))

  merge_sdf <- collection_sdf %>%
    group_by(path) %>%
    summarise(tile = rst_merge_agg(tile)) %>%
    mutate(extent = st_astext(rst_boundingbox(tile)))

  expect_equal(sdf_nrow(merge_sdf), 1)
  expect_equal(
    collection_sdf %>% head(1) %>% collect %>% .$extent,
    merge_sdf %>% head(1) %>% collect %>% .$extent
  )

  combine_avg_sdf <- collection_sdf %>%
    group_by(path) %>%
    summarise(tile = rst_combineavg_agg(tile)) %>%
    mutate(extent = st_astext(rst_boundingbox(tile)))

  expect_equal(sdf_nrow(combine_avg_sdf), 1)
  expect_equal(
    collection_sdf %>% head(1) %>% collect %>% .$extent,
    combine_avg_sdf %>% head(1) %>% collect %>% .$extent
  )

})

test_that("the tessellate-join-clip-merge flow works on NetCDF files", {
  target_resolution <- 1L

  region_keys <- c("NAME", "STATE", "BOROUGH", "BLOCK", "TRACT")

  census_sdf <- spark_read_source(
    sc,
    name = "census_raw",
    source = "com.databricks.labs.mosaic.datasource.OGRFileFormat",
    path = "data/Blocks2020.zip",
    options = list(
      "vsizip" = "true",
      "chunkSize" = "20"
    )
  ) %>%
    select(region_keys, geom_0, geom_0_srid) %>%
    distinct() %>%
    mutate(geom_0 = st_simplify(geom_0, as.double(0.001))) %>%
    mutate(geom_0 = st_updatesrid(geom_0, geom_0_srid, 4326L)) %>%
    mutate(chip = grid_tessellateexplode(geom_0, target_resolution)) %>%
    sdf_select(region_keys, chip$is_core, chip$index_id, chip$wkb)

  raster_sdf <-
    spark_read_source(
      sc,
      name = "raster_raw",
      source = "gdal",
      path = "data/prAdjust_day_HadGEM2-CC_SMHI-DBSrev930-GFD-1981-2010-postproc_rcp45_r1i1p1_20201201-20201231.nc",
      options = list("raster.read.strategy" = "retile_on_read")
    ) %>%
      mutate(tile = rst_separatebands(tile)) %>%
      sdf_register("raster")

  indexed_raster_sdf <- sdf_sql(sc, "SELECT tile, element_at(rst_metadata(tile), 'NC_GLOBAL#GDAL_MOSAIC_BAND_INDEX') as timestep FROM raster") %>%
    filter(timestep == 21L) %>%
    mutate(tile = rst_setsrid(tile, 4326L)) %>%
    mutate(tile = rst_to_overlapping_tiles(tile, 20L, 20L, 10L)) %>%
    mutate(tile = rst_tessellate(tile, target_resolution))

  clipped_sdf <- indexed_raster_sdf %>%
    sdf_select(tile, tile.index_id, timestep, .drop_parents = FALSE) %>%
    inner_join(census_sdf, by = "index_id") %>%
    mutate(tile = rst_clip(tile, wkb))

  merged_precipitation <- clipped_sdf %>%
    group_by(region_keys, timestep) %>%
    summarise(tile = rst_merge_agg(tile))

  expect_equal(sdf_nrow(merged_precipitation), 1)
})