# Update the auto-generated code files 
setwd("/Users/robert.whiffin/Documents/mosaic/R/SparkR")
library(devtools)
library(roxygen2)
#devtools::create("sparkrMosaic")
scala_file_path <- "/Users/robert.whiffin/Documents/mosaic/src/main/scala/com/databricks/labs/mosaic/functions/MosaicContext.scala"

system_cmd <- paste0(c("Rscript --vanilla generate_sparkr_functions.R", scala_file_path), collapse = " ")

system(system_cmd)

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

increment_minor_version_number()

#system("R CMD build sparkrMosaic")
devtools::document("sparkrMosaic")



devtools::build("sparkrMosaic")
#



Sys.setenv("SPARK_HOME"="/usr/local/Cellar/apache-spark/3.2.1/libexec")
Sys.setenv("JAVA_HOME"="/usr/local/Cellar/openjdk@8/1.8.0+312/libexec/openjdk.jdk/Contents/Home")
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
spark = sparkR.session(
  master = "local[*]"
  ,sparkJars = "/Users/robert.whiffin/Downloads/artefacts/mosaic-0.1.0-SNAPSHOT-jar-with-dependencies.jar"
)

#geometry_api <- sparkR.callJStatic(x="com.databricks.labs.mosaic.core.geometry.api.GeometryAPI", methodName="apply", "ESRI")
#index_system_id <- sparkR.callJStatic(x="com.databricks.labs.mosaic.core.index.IndexSystemID", methodName="apply", "H3")
#indexing_system <- sparkR.callJStatic(x="com.databricks.labs.mosaic.core.index.IndexSystemID", methodName="getIndexSystem", index_system_id)
#mosaic_context <- sparkR.newJObject(x="com.databricks.labs.mosaic.functions.MosaicContext", indexing_system, geometry_api)
#functions <- sparkR.callJMethod(mosaic_context, "functions")
#
#
#
#source("sparkrMosaic/R/generics.R")
#source("sparkrMosaic/R/functions.R")
devtools::load_all("sparkrMosaic")

enableMosaic()

sdf = SparkR::createDataFrame(
  data.frame(
    index = 1:5, 
    X=as.double(0:4)
    ,Y=as.double(0:4)
  )
)
sdf = SparkR::withColumn(sdf, "points", st_point(column("X"), column("Y")))

SparkR::collect(
  SparkR::withColumn(sdf, "length", st_length(st_aswkt(column("points"))))
)

