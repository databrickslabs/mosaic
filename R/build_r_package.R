repo<-"https://cran.ma.imperial.ac.uk/"

install.packages("devtools", repos=repo)
install.packages("roxygen2", repos=repo)
install.packages("sparklyr", repos=repo)
devtools::install_github("apache/spark@v3.2.1", subdir='R/pkg')

library(devtools)
library(roxygen2)
library(SparkR)
library(sparklyr)
SparkR::install.spark()



build_mosaic_bindings <- function(){
  # build functions
  scala_file_path <- "../src/main/scala/com/databricks/labs/mosaic/functions/MosaicContext.scala"
  system_cmd <- paste0(c("Rscript --vanilla generate_R_bindings.R", scala_file_path), collapse = " ")
  system(system_cmd)

  # build doc
  devtools::document("sparkR-mosaic/sparkrMosaic")
  devtools::document("sparklyr-mosaic/sparklyrMosaic")

  ## build package
  devtools::build("sparkR-mosaic/sparkrMosaic")
  devtools::build("sparklyr-mosaic/sparklyrMosaic")
  
}


build_mosaic_bindings()
