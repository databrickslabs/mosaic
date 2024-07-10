spark_location <- Sys.getenv("SPARK_HOME")
library(SparkR, lib.loc = c(file.path(spark_location, "R", "lib")))
library(pkgbuild)

build_mosaic_bindings <- function(){
  ## build package
  pkgbuild::build("sparkR-mosaic/sparkrMosaic")
  pkgbuild::build("sparklyr-mosaic/sparklyrMosaic")
  
}

build_mosaic_bindings()
