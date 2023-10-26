spark_location <- "/usr/spark-download/unzipped/spark-3.2.1-bin-hadoop2.7"
Sys.setenv(SPARK_HOME = spark_location)

library(SparkR, lib.loc = c(file.path(spark_location, "R", "lib")))
library(roxygen2)

build_mosaic_docs <- function(){
  # build doc
  roxygen2::roxygenize("sparkR-mosaic/sparkrMosaic")
  roxygen2::roxygenize("sparklyr-mosaic/sparklyrMosaic")

}

build_mosaic_docs()