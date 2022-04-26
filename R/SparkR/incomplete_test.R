library(testthat)

testthat::test_that("sparkrMosaic works", {
  
  mosaic_jar <- list.files("staging")
  print("mosaic_jar")
  mosaic_jar <- mosaic_jar[grep("SNAPSHOT-jar-with-dependencies.jar", mosaic_jar, fixed=T)]
  spark = sparkR.session(
    master <- "local[*]"
    ,sparkJars <- paste0("staging/", mosaic_jar)
  )
  
  
  enableMosaic()
  
  sdf = SparkR::createDataFrame(
    data.frame(
      index = 1:5, 
      X=as.double(0:4)
      ,Y=as.double(0:4)
    )
  )
  
  points = SparkR::withColumn(sdf, "points", st_astext(st_point(column("X"), column("Y"))))
  collect(points)
  
  sdf = SparkR::createDataFrame(
    data.frame(
      wkt = c("POLYGON ((0 0, 0 2, 1 2, 1 0, 0 0))")
      ,point_wkt = c("POINT (1 1)")
      )
  )
  functions = ls("package:sparkrMosaic")
  exclusions = c("enableMosaic", "st_astext", "st_point")
  
  
  
})

