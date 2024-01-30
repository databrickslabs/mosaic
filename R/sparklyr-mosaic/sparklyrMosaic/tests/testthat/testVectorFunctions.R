source("data.R")

test_that("scalar vector functions behave as intended", {

  sdf <- sdf_copy_to(
    sc,
    data.frame(
      wkt = "POLYGON ((2 1, 1 2, 2 3, 2 1))",
      point_wkt = "POINT (1 1)"
    )
  )

  sdf <- mutate(sdf, "st_area" = st_area(wkt))
  sdf <- mutate(sdf, "st_length" = st_length(wkt))
  sdf <- mutate(sdf, "st_perimeter" = st_perimeter(wkt))
  sdf <- mutate(sdf, "st_buffer" = st_buffer(wkt, as.double(1.1)))
  sdf <- mutate(sdf, "st_bufferloop" = st_bufferloop(wkt, as.double(1.1), as.double(1.2)))
  sdf <- mutate(sdf, "st_convexhull" = st_convexhull(wkt))
  sdf <- mutate(sdf, "st_dump" = st_dump(wkt))
  sdf <- mutate(sdf, "st_translate" = st_translate(wkt, 1L, 1L))
  sdf <- mutate(sdf, "st_scale" = st_scale(wkt, 1L, 1L))
  sdf <- mutate(sdf, "st_rotate" = st_rotate(wkt, 1L))
  sdf <- mutate(sdf, "st_centroid" = st_centroid(wkt))
  sdf <- mutate(sdf, "st_numpoints" = st_numpoints(wkt))
  sdf <- mutate(sdf, "st_haversine" = st_haversine(as.double(0.0), as.double(90.0), as.double(0.0), as.double(0.0)))
  sdf <- mutate(sdf, "st_isvalid" = st_isvalid(wkt))
  sdf <- mutate(sdf, "st_hasvalidcoordinates" = st_hasvalidcoordinates(wkt, "EPSG:2192", "bounds"))
  sdf <- mutate(sdf, "st_intersects" = st_intersects(wkt, wkt))
  sdf <- mutate(sdf, "st_intersection" = st_intersection(wkt, wkt))
  sdf <- mutate(sdf, "st_envelope" = st_envelope(wkt))
  sdf <- mutate(sdf, "st_simplify" = st_simplify(wkt, as.double(0.001)))
  sdf <- mutate(sdf, "st_difference" = st_difference(wkt, wkt))
  sdf <- mutate(sdf, "st_union" = st_union(wkt, wkt))
  sdf <- mutate(sdf, "st_unaryunion" = st_unaryunion(wkt))
  sdf <- mutate(sdf, "st_geometrytype" = st_geometrytype(wkt))
  sdf <- mutate(sdf, "st_xmin" = st_xmin(wkt))
  sdf <- mutate(sdf, "st_xmax" = st_xmax(wkt))
  sdf <- mutate(sdf, "st_ymin" = st_ymin(wkt))
  sdf <- mutate(sdf, "st_ymax" = st_ymax(wkt))
  sdf <- mutate(sdf, "st_zmin" = st_zmin(wkt))
  sdf <- mutate(sdf, "st_zmax" = st_zmax(wkt))
  sdf <- mutate(sdf, "flatten_polygons" = flatten_polygons(wkt))

  # SRID functions

  sdf <- mutate(sdf, "geom_with_srid" = st_setsrid(st_geomfromwkt(wkt), 4326L))
  sdf <- mutate(sdf, "srid_check" = st_srid(geom_with_srid))
  sdf <- mutate(sdf, "transformed_geom" = st_transform(geom_with_srid, 3857L))

  # Grid functions

  sdf <- mutate(sdf, "grid_longlatascellid" = grid_longlatascellid(as.double(1L), as.double(1L), 1L))
  sdf <- mutate(sdf, "grid_pointascellid" = grid_pointascellid(point_wkt, 1L))
  sdf <- mutate(sdf, "grid_boundaryaswkb" = grid_boundaryaswkb(grid_longlatascellid))
  sdf <- mutate(sdf, "grid_polyfill" = grid_polyfill(wkt, 1L))
  sdf <- mutate(sdf, "grid_tessellateexplode" = grid_tessellateexplode(wkt, 1L))
  sdf <- mutate(sdf, "grid_tessellateexplode_no_core_chips" = grid_tessellateexplode(wkt, 1L, FALSE))
  sdf <- mutate(sdf, "grid_tessellate" = grid_tessellate(wkt, 1L))
  sdf <- mutate(sdf, "grid_cellarea" = grid_cellarea(grid_longlatascellid))

  expect_no_error(spark_write_source(sdf, "noop", mode = "overwrite"))
  expect_equal(sdf_nrow(sdf), 1)

})

test_that("aggregate vector functions behave as intended", {

  sdf <- sdf_sql(sc, "SELECT id as location_id FROM range(1)") %>%
    mutate(geometry = st_geomfromgeojson(inputGJ))
  expect_equal(sdf_nrow(sdf), 1)

  sdf.l <- sdf %>%
    select(
      left_id = location_id,
      left_geom = geometry
    ) %>%
    mutate(left_index = mosaic_explode(left_geom, 11L))

  sdf.r <- sdf %>%
    select(
      right_id = location_id,
      right_geom = geometry
    ) %>%
    mutate(right_geom = st_translate(
      right_geom,
      st_area(right_geom) * runif(n()) * 0.1,
      st_area(right_geom) * runif(n()) * 0.1)
    ) %>%
    mutate(right_index = mosaic_explode(right_geom, 11L))

  sdf.intersection <- sdf.l %>%
    inner_join(sdf.r, by = c("left_index" = "right_index"), keep = TRUE) %>%
    dplyr::group_by(left_id, right_id) %>%
    dplyr::summarise(
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