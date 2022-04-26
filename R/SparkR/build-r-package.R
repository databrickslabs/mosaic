install.packages("devtools")
install.packages("roxygen")
install.packages("SparkR")
library(devtools)
library(roxygen2)
library(SparkR)

# Increment the minor version number in the DESCRIPTION.TEMPLATE and copy into the package folder
increment_minor_version_number <- function(){
  description_file <- file("sparkrMosaic/DESCRIPTION")
  description <- readLines(description_file)
  
  version_index <- grep("Version", description, fixed=T)
  version_details <- strsplit(description[version_index], ".", fixed=T)
  minor_number <- as.integer(version_details[[1]][2])
  minor_number <- minor_number + 1
  version_details[[1]][2] <- as.character(minor_number)
  version_details <- paste0(unlist(version_details), collapse = ".")
  description[version_index] = version_details
  writeLines(description, description_file)
  closeAllConnections()
}


build_sparkr_mosaic <- function(){
  # build functions
  scala_file_path <- "../../src/main/scala/com/databricks/labs/mosaic/functions/MosaicContext.scala"
  system_cmd <- paste0(c("Rscript --vanilla generate_sparkr_functions.R", scala_file_path), collapse = " ")
  system(system_cmd)
  
  # build doc
  devtools::document("sparkrMosaic")
  
  # increment version number
  increment_minor_version_number()
  
  # run check
  # devtools::check("sparkrMosaic")
  
  # build package
  devtools::build("sparkrMosaic")
}


build_sparkr_mosaic()