spark_location <- "/usr/spark-download/unzipped/spark-3.3.2-bin-hadoop3"
Sys.setenv(SPARK_HOME = spark_location)

library(SparkR, lib.loc = c(file.path(spark_location, "R", "lib")))
library(roxygen2)

build_mosaic_docs <- function(){
  # build doc
  roxygen2::roxygenize("sparkR-mosaic/sparkrMosaic")
  roxygen2::roxygenize("sparklyr-mosaic/sparklyrMosaic")

}

build_mosaic_docs()