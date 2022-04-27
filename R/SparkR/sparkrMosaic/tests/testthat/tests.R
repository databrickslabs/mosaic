testthat::test_that("bindings run",{
testthat::expect_equal(1.0, {
  staging_dir = "/home/runner/work/mosaic/mosaic/staging/"
  mosaic_jar <- list.files(staging_dir)
  mosaic_jar <- mosaic_jar[grep("SNAPSHOT-jar-with-dependencies.jar", mosaic_jar, fixed=T)]
  print("Looking for mosaic jar in")
  print(mosaic_jar)
  spark = sparkR.session(
    master <- "local[*]"
    ,sparkJars <- paste0(staging_dir, mosaic_jar)
  )
  
  enableMosaic()
  
  sdf <- SparkR::createDataFrame(
    data.frame(
      wkt = "POLYGON ((0 0, 0 2, 1 2, 1 0, 0 0))",
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
  sdf <- withColumn(sdf, "st_centroid2D", st_centroid2D(column("wkt")))
  sdf <- withColumn(sdf, "st_centroid3D", st_centroid3D(column("wkt")))
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
  sdf <- withColumn(sdf, "point_index_lonlat", point_index_lonlat(lit(1), lit(1), lit(1L)))
  sdf <- withColumn(sdf, "point_index_geom", point_index_geom(column("point_wkt"), lit(1)))
  sdf <- withColumn(sdf, "index_geometry", index_geometry(lit(1)))
  sdf <- withColumn(sdf, "polyfill", polyfill(column("wkt"), lit(1L)))
  sdf <- withColumn(sdf, "mosaic_explode", mosaic_explode(column("wkt"), lit(1L)))
  sdf <- withColumn(sdf, "mosaicfill", mosaicfill(column("wkt"), lit(1L)))
  sdf <- withColumn(sdf, "geom_with_srid", st_setsrid(st_geomfromwkt(column("wkt")), lit(4326)))
  sdf <- withColumn(sdf, "srid_check", st_srid(column("geom_with_srid")))
  sdf <- withColumn(sdf, "transformed_geom", st_transform(column("geom_with_srid"), lit(3857)))
  as.double(SparkR::count(sdf))
  })
  })


  