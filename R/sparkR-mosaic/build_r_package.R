repo<-"https://cran.ma.imperial.ac.uk/"

#install.packages("devtools", repos=repo)
install.packages("pkgbuild", repos=repo)
install.packages("roxygen2", repos=repo)

spark_location <- "/usr/spark-download/unzipped/spark-3.2.1-bin-hadoop2.7"
Sys.setenv(SPARK_HOME = spark_location)
#library(devtools)
library(pkgbuild)
library(roxygen2)
library(SparkR, lib.loc = c(file.path(spark_location, "R", "lib")))


build_sparkr_mosaic <- function(){
  # build functions
  scala_file_path <- "../../src/main/scala/com/databricks/labs/mosaic/functions/MosaicContext.scala"
  system_cmd <- paste0(c("Rscript --vanilla generate_sparkr_functions.R", scala_file_path), collapse = " ")
  system(system_cmd)

  # build doc
  roxygen2::roxygenize("sparkrMosaic")

  ## build package
  pkgbuild::build("sparkrMosaic")
  
}


build_sparkr_mosaic()
