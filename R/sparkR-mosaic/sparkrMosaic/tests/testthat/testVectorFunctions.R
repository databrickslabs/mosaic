test_that("scalar vector functions behave as intended", {
  sdf <- createDataFrame(
    data.frame(
      wkt = "POLYGON ((2 1, 1 2, 2 3, 2 1))",
      point_wkt = "POINT (1 1)"
    )
  )

  sdf <- withColumn(sdf, "st_area", st_area(column("wkt")))
  sdf <- withColumn(sdf, "st_length", st_length(column("wkt")))
  sdf <- withColumn(sdf, "st_perimeter", st_perimeter(column("wkt")))
  sdf <- withColumn(sdf, "st_convexhull", st_convexhull(column("wkt")))
  sdf <- withColumn(sdf, "st_dump", st_dump(column("wkt")))
  sdf <- withColumn(sdf, "st_translate", st_translate(column("wkt"), lit(1), lit(1)))
  sdf <- withColumn(sdf, "st_scale", st_scale(column("wkt"), lit(1), lit(1)))
  sdf <- withColumn(sdf, "st_rotate", st_rotate(column("wkt"), lit(1)))
  sdf <- withColumn(sdf, "st_centroid", st_centroid(column("wkt")))
  sdf <- withColumn(sdf, "st_length", st_length(column("wkt")))
  sdf <- withColumn(sdf, "st_isvalid", st_isvalid(column("wkt")))
  sdf <- withColumn(sdf, "st_intersects", st_intersects(column("wkt"), column("wkt")))
  sdf <- withColumn(sdf, "st_intersection", st_intersection(column("wkt"), column("wkt")))
  sdf <- withColumn(sdf, "st_geometrytype", st_geometrytype(column("wkt")))
  sdf <- withColumn(sdf, "st_isvalid", st_isvalid(column("wkt")))
  sdf <- withColumn(sdf, "st_xmin", st_xmin(column("wkt")))
  sdf <- withColumn(sdf, "st_xmax", st_xmax(column("wkt")))
  sdf <- withColumn(sdf, "st_ymin", st_ymin(column("wkt")))
  sdf <- withColumn(sdf, "st_ymax", st_ymax(column("wkt")))
  sdf <- withColumn(sdf, "st_zmin", st_zmin(column("wkt")))
  sdf <- withColumn(sdf, "st_zmax", st_zmax(column("wkt")))
  sdf <- withColumn(sdf, "flatten_polygons", flatten_polygons(column("wkt")))

  # SRID
  sdf <- withColumn(sdf, "geom_with_srid", st_setsrid(st_geomfromwkt(column("wkt")), lit(4326L)))
  sdf <- withColumn(sdf, "srid_check", st_srid(column("geom_with_srid")))
  sdf <- withColumn(sdf, "transformed_geom", st_transform(column("geom_with_srid"), lit(3857L)))

  # Grid functions
  sdf <- withColumn(sdf, "grid_longlatascellid", grid_longlatascellid(lit(1), lit(1), lit(1L)))
  sdf <- withColumn(sdf, "grid_pointascellid", grid_pointascellid(column("point_wkt"), lit(1L)))
  sdf <- withColumn(sdf, "grid_boundaryaswkb", grid_boundaryaswkb(column("grid_pointascellid")))
  sdf <- withColumn(sdf, "grid_polyfill", grid_polyfill(column("wkt"), lit(1L)))
  sdf <- withColumn(sdf, "grid_tessellateexplode", grid_tessellateexplode(column("wkt"), lit(1L)))
  sdf <- withColumn(sdf, "grid_tessellate", grid_tessellate(column("wkt"), lit(1L)))

  # Deprecated
  sdf <- withColumn(sdf, "point_index_lonlat", point_index_lonlat(lit(1), lit(1), lit(1L)))
  sdf <- withColumn(sdf, "point_index_geom", point_index_geom(column("point_wkt"), lit(1L)))
  sdf <- withColumn(sdf, "index_geometry", index_geometry(column("point_index_geom")))
  sdf <- withColumn(sdf, "polyfill", polyfill(column("wkt"), lit(1L)))
  sdf <- withColumn(sdf, "mosaic_explode", mosaic_explode(column("wkt"), lit(1L)))
  sdf <- withColumn(sdf, "mosaicfill", mosaicfill(column("wkt"), lit(1L)))

  expect_no_error(write.df(sdf, source = "noop", mode = "overwrite"))
  expect_equal(nrow(sdf), 1)

})

test_that("aggregate vector functions behave as intended", {

  sdf <- sql("SELECT id as location_id FROM range(1)")

  inputGJ <- read_file("data/boroughs.geojson")
  sdf <- withColumn(sdf, "geometry", st_geomfromgeojson(lit(inputGJ)))
  expect_equal(nrow(sdf), 1)

  sdf.l <- select(sdf, alias(column("location_id"), "left_id"), alias(column("geometry"), "left_geom"))
  sdf.l <- withColumn(sdf.l, "left_index", mosaic_explode(column("left_geom"), lit(11L)))

  sdf.r <- select(sdf, alias(column("location_id"), "right_id"), alias(column("geometry"), "right_geom"))
  sdf.r <- withColumn(sdf.r, "right_geom", st_translate(
    column("right_geom"),
    st_area(column("right_geom")) * runif(1) * 0.1,
    st_area(column("right_geom")) * runif(1) * 0.1
  )
  )
  sdf.r <- withColumn(sdf.r, "right_index", mosaic_explode(column("right_geom"), lit(11L)))

  sdf.intersection <- join(sdf.l, sdf.r, sdf.l$left_index == sdf.r$right_index, "inner")
  sdf.intersection <- summarize(
    groupBy(sdf.intersection, sdf.intersection$left_id, sdf.intersection$right_id),
    agg_intersects = st_intersects_agg(column("left_index"), column("right_index")),
    agg_intersection = st_intersection_agg(column("left_index"), column("right_index")),
    left_geom = first(column("left_geom")),
    right_geom = first(column("right_geom"))
  )
  sdf.intersection <- withColumn(sdf.intersection, "flat_intersects", st_intersects(column("left_geom"), column("right_geom")))
  sdf.intersection <- withColumn(sdf.intersection, "comparison_intersects", column("agg_intersects") == column("flat_intersects"))
  sdf.intersection <- withColumn(sdf.intersection, "agg_area", st_area(column("agg_intersection")))
  sdf.intersection <- withColumn(sdf.intersection, "flat_intersection", st_intersection(column("left_geom"), column("right_geom")))
  sdf.intersection <- withColumn(sdf.intersection, "flat_area", st_area(column("flat_intersection")))
  sdf.intersection <- withColumn(sdf.intersection, "comparison_intersection", abs(column("agg_area") - column("flat_area")) <= lit(1e-3))

  expect_true(first(sdf.intersection)$comparison_intersects)
  expect_true(first(sdf.intersection)$comparison_intersection)

})

test_that("triangulation / interpolation functions behave as intended", {
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
triangulation_sdf <- withColumn(sdf, "triangles", st_triangulate(column("masspoints"), column("breaklines"), lit(0.0), lit(0.01), lit("NONENCROACHING")))
cache(triangulation_sdf)
expect_equal(SparkR::count(triangulation_sdf), 2)
expected <- c("POLYGON Z((0 2 2, 2 1 0, 1 3 3, 0 2 2))", "POLYGON Z((1 3 3, 2 1 0, 3 2 1, 1 3 3))")
expect_contains(expected, first(triangulation_sdf)$triangles)

interpolation_sdf <- sdf
interpolation_sdf <- withColumn(interpolation_sdf, "origin", st_geomfromwkt(lit("POINT (0.55 1.75)")))
interpolation_sdf <- withColumn(interpolation_sdf, "xWidth", lit(12L))
interpolation_sdf <- withColumn(interpolation_sdf, "yWidth", lit(6L))
interpolation_sdf <- withColumn(interpolation_sdf, "xSize", lit(0.1))
interpolation_sdf <- withColumn(interpolation_sdf, "ySize", lit(0.1))
interpolation_sdf <- withColumn(interpolation_sdf, "interpolated", st_interpolateelevation(
  column("masspoints"),
  column("breaklines"),
  lit(0.01),
  lit(0.01),
  lit("NONENCROACHING"),
  column("origin"),
  column("xWidth"),
  column("yWidth"),
  column("xSize"),
  column("ySize")
))
cache(interpolation_sdf)
expect_equal(SparkR::count(interpolation_sdf), 6 * 12)
expect_contains(collect(interpolation_sdf)$interpolated, "POINT Z(1.6 1.8 1.2)")
})
