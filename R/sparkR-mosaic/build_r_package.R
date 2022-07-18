repo<-"https://cran.ma.imperial.ac.uk/"

install.packages("devtools", repos=repo)
install.packages("roxygen2", repos=repo)
devtools::install_github("apache/spark@v3.2.1", subdir='R/pkg')

library(devtools)
library(roxygen2)
library(SparkR)
SparkR::install.spark(mirrorUrl="https://archive.apache.org/dist/spark")


build_sparkr_mosaic <- function(){
  # build functions
  scala_file_path <- "../../src/main/scala/com/databricks/labs/mosaic/functions/MosaicContext.scala"
  system_cmd <- paste0(c("Rscript --vanilla generate_sparkr_functions.R", scala_file_path), collapse = " ")
  system(system_cmd)

  # build doc
  devtools::document("sparkrMosaic")

  ## build package
  devtools::build("sparkrMosaic")
  
}


build_sparkr_mosaic()
