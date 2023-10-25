library(roxygen2)

build_mosaic_docs <- function(){
  # build doc
  roxygen2::roxygenize("sparkR-mosaic/sparkrMosaic")
  roxygen2::roxygenize("sparklyr-mosaic/sparklyrMosaic")

}

build_mosaic_docs()