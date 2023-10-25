spark_location <- "/usr/spark-download/unzipped/spark-3.2.1-bin-hadoop2.7"
Sys.setenv(SPARK_HOME = spark_location)

library(SparkR, lib.loc = c(file.path(spark_location, "R", "lib")))


library(pkgbuild)
library(sparklyr)



build_mosaic_bindings <- function(){
  ## build package
  pkgbuild::build("sparkR-mosaic/sparkrMosaic")
  pkgbuild::build("sparklyr-mosaic/sparklyrMosaic")
  
}

build_mosaic_bindings()
