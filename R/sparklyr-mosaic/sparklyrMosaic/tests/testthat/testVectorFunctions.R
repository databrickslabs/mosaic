options(warn = -1)

test_that("scalar vector functions behave as intended", {
  sdf_raw <- sdf_copy_to(
    sc,
    data.frame(
      wkt = "POLYGON ((2 1, 1 2, 2 3, 2 1))",
      point_wkt = "POINT (1 1)")
    )

  sdf <- sdf_raw %>% mutate(
    st_area = st_area(wkt),
    st_length = st_length(wkt),
    st_perimeter = st_perimeter(wkt),
    st_buffer = st_buffer(wkt, as.double(1.1)),
    st_buffer_optparams = st_buffer(wkt, as.double(1.1), "endcap=square quad_segs=2"),
    st_bufferloop = st_bufferloop(wkt, as.double(1.1), as.double(1.2)),
    st_convexhull = st_convexhull(wkt),
    st_dump = st_dump(wkt),
    st_translate = st_translate(wkt, 1L, 1L),
    st_scale = st_scale(wkt, 1L, 1L),
    st_rotate = st_rotate(wkt, 1L),
    st_centroid = st_centroid(wkt),
    st_numpoints = st_numpoints(wkt),
    st_haversine = st_haversine(
      as.double(0.0),
      as.double(90.0),
      as.double(0.0),
      as.double(0.0)
    ),
    st_isvalid = st_isvalid(wkt),
    st_hasvalidcoordinates = st_hasvalidcoordinates(wkt, "EPSG:2192", "bounds"),
    st_intersects = st_intersects(wkt, wkt),
    st_intersection = st_intersection(wkt, wkt),
    st_envelope = st_envelope(wkt),
    st_simplify = st_simplify(wkt, as.double(0.001)),
    st_difference = st_difference(wkt, wkt),
    st_union = st_union(wkt, wkt),
    st_unaryunion = st_unaryunion(wkt),
    st_geometrytype = st_geometrytype(wkt),
    st_xmin = st_xmin(wkt),
    st_xmax = st_xmax(wkt),
    st_ymin = st_ymin(wkt),
    st_ymax = st_ymax(wkt),
    st_zmin = st_zmin(wkt),
    st_zmax = st_zmax(wkt)
  )

  expect_no_error(spark_write_source(sdf, "noop", mode = "overwrite"))
  expect_equal(sdf_nrow(sdf), 1)

  # SRID functions

  sdf <- sdf_raw %>% mutate(
    geom_with_srid = st_setsrid(st_geomfromwkt(wkt), 4326L),
    srid_check = st_srid(geom_with_srid),
    transformed_geom = st_transform(geom_with_srid, 3857L)
  )

  expect_no_error(spark_write_source(sdf, "noop", mode = "overwrite"))
  expect_equal(sdf_nrow(sdf), 1)

  # Grid functions

  sdf <- sdf_raw %>% mutate(
    grid_longlatascellid = grid_longlatascellid(as.double(1L), as.double(1L), 1L),
    grid_pointascellid = grid_pointascellid(point_wkt, 1L),
    grid_boundaryaswkb = grid_boundaryaswkb(grid_longlatascellid),
    grid_polyfill = grid_polyfill(wkt, 1L),
    grid_tessellateexplode = grid_tessellateexplode(wkt, 1L),
    grid_tessellate = grid_tessellate(wkt, 1L),
    grid_cellarea = grid_cellarea(grid_longlatascellid)
  )

  expect_no_error(spark_write_source(sdf, "noop", mode = "overwrite"))
  expect_equal(sdf_nrow(sdf), 1)

})

test_that("aggregate vector functions behave as intended", {
  inputGJ <- read_file("data/boroughs.geojson")
  sdf <- sdf_sql(sc, "SELECT id as location_id FROM range(1)") %>%
    mutate(geometry = st_geomfromgeojson(inputGJ))
  expect_equal(sdf_nrow(sdf), 1)

  sdf.l <- sdf %>%
    select(left_id = location_id, left_geom = geometry) %>%
    mutate(left_index = mosaic_explode(left_geom, 11L))

  sdf.r <- sdf %>%
    select(right_id = location_id, right_geom = geometry) %>%
    mutate(right_geom = st_translate(
      right_geom,
      st_area(right_geom) * runif(n()) * 0.1,
      st_area(right_geom) * runif(n()) * 0.1
    )) %>%
    mutate(right_index = mosaic_explode(right_geom, 11L))

  sdf.intersection <- sdf.l %>%
    inner_join(sdf.r,
               by = c("left_index" = "right_index"),
               keep = TRUE) %>%
    group_by(left_id, right_id) %>%
    summarise(
      agg_intersects = st_intersects_agg(left_index, right_index),
      agg_intersection = st_intersection_agg(left_index, right_index),
      left_geom = max(left_geom, 1),
      right_geom = max(right_geom, 1)
    ) %>%
    mutate(
      flat_intersects = st_intersects(left_geom, right_geom),
      comparison_intersects = agg_intersects == flat_intersects,
      agg_area = st_area(agg_intersection),
      flat_intersection = st_intersection(left_geom, right_geom),
      flat_area = st_area(flat_intersection),
      comparison_intersection = abs(agg_area - flat_area) <= 1e-3
    )

  expect_no_error(spark_write_source(sdf.intersection, "noop", mode = "overwrite"))
  expect_true(sdf.intersection %>% head(1) %>% sdf_collect %>% .$comparison_intersects)
  expect_true(sdf.intersection %>% head(1) %>% sdf_collect %>% .$comparison_intersection)


})

test_that ("triangulation and interpolation functions behave as intended", {
  sdf <- sdf_copy_to(sc, data.frame(
    wkt = c("POINT Z (3 2 1)", "POINT Z (2 1 0)", "POINT Z (1 3 3)", "POINT Z (0 2 2)")
  ))

  sdf <- sdf %>%
    group_by() %>%
    summarise(masspoints = collect_list(wkt)) %>%
    mutate(breaklines = array("LINESTRING EMPTY"))

  triangulation_sdf <- sdf %>%
    mutate(triangles = st_triangulate(masspoints, breaklines, as.double(0.00), as.double(0.01), "NONENCROACHING"))

  expect_equal(sdf_nrow(triangulation_sdf), 2)

  expected <- c("POLYGON Z((0 2 2, 2 1 0, 1 3 3, 0 2 2))",
                "POLYGON Z((1 3 3, 2 1 0, 3 2 1, 1 3 3))")
  expect_contains(expected, sdf_collect(triangulation_sdf)$triangles[0])

  interpolation_sdf <- sdf %>%
    mutate(
      origin = st_geomfromwkt("POINT (0.55 1.75)"),
      xWidth = 12L,
      yWidth = 6L,
      xSize = as.double(0.1),
      ySize = as.double(0.1),
      interpolated = st_interpolateelevation(
        masspoints,
        breaklines,
        as.double(0.01),
        as.double(0.01),
        "NONENCROACHING",
        origin,
        xWidth,
        yWidth,
        xSize,
        ySize
      )
    )
  expect_equal(sdf_nrow(interpolation_sdf), 6 * 12)
  expect_contains(sdf_collect(interpolation_sdf)$interpolated,
                  "POINT Z(1.6 1.8 1.2)")
})
