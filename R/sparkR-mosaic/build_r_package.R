repo<-"https://cran.rstudio.com/"

install.packages("devtools", repos=repo)
install.packages("roxygen2", repos=repo)
devtools::install_github("apache/spark@v3.2.1", subdir='R/pkg')

library(devtools)
library(roxygen2)
library(SparkR)
SparkR::install.spark()


build_sparkr_mosaic <- function(){
  # build functions
  scala_file_path <- "../../src/main/scala/com/databricks/labs/mosaic/functions/MosaicContext.scala"
  system_cmd <- paste0(c("Rscript --vanilla generate_sparkr_functions.R", scala_file_path), collapse = " ")
  system(system_cmd)

  # copy enableMosaic functions in sparkrMosaic
  system_cmd <- "cp enableMosaic.R ./sparkrMosaic/R/enableMosaic.R"
  system(system_cmd)

  # build doc
  devtools::document("sparkrMosaic")

  ## build package
  devtools::build("sparkrMosaic")
  
  
  # run test
  system_cmd <- "Rscript --vanilla tests.R"
  system(system_cmd)
}


build_sparkr_mosaic()