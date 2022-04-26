# Update the auto-generated code files 
setwd("/Users/robert.whiffin/Documents/mosaic/R/SparkR")
library(devtools)
library(roxygen2)
devtools::create("sparkrMosaic")
scala_file_path <- "/Users/robert.whiffin/Documents/mosaic/src/main/scala/com/databricks/labs/mosaic/functions/MosaicContext.scala"

system_cmd <- paste0(c("Rscript --vanilla generate_sparkr_functions.R", scala_file_path), collapse = " ")

system(system_cmd)

# Increment the version number in the DESCRIPTION.TEMPLATE and copy into the package folder

# Update the 



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

geometry_api <- sparkR.callJStatic(x="com.databricks.labs.mosaic.core.geometry.api.GeometryAPI", methodName="apply", "ESRI")
index_system_id <- sparkR.callJStatic(x="com.databricks.labs.mosaic.core.index.IndexSystemID", methodName="apply", "H3")
indexing_system <- sparkR.callJStatic(x="com.databricks.labs.mosaic.core.index.IndexSystemID", methodName="getIndexSystem", index_system_id)
mosaic_context <- sparkR.newJObject(x="com.databricks.labs.mosaic.functions.MosaicContext", indexing_system, geometry_api)
functions <- sparkR.callJMethod(mosaic_context, "functions")



source("sparkrMosaic/R/generics.R")
source("sparkrMosaic/R/functions.R")

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

