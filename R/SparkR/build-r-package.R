Sys.setenv("SPARK_HOME"="/usr/local/Cellar/apache-spark/3.2.1/libexec")
Sys.setenv("JAVA_HOME"="/usr/local/Cellar/openjdk@8/1.8.0+312/libexec/openjdk.jdk/Contents/Home")
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
spark = sparkR.session(
  master = "local[*]"
  ,sparkJars = "/Users/robert.whiffin/Downloads/artefacts/mosaic-0.1.0-SNAPSHOT-jar-with-dependencies.jar"
  )

setwd("/Users/robert.whiffin/Documents/mosaic/R/SparkR")
system("R CMD BATCH generate_sparkr_functions.R")

system("R CMD build sparkrMosaic")
library(devtools)
devtools::build("sparkrMosaic")

geometry_api <- sparkR.callJStatic(x="com.databricks.labs.mosaic.core.geometry.api.GeometryAPI", methodName="apply", "ESRI")
index_system_id <- sparkR.callJStatic(x="com.databricks.labs.mosaic.core.index.IndexSystemID", methodName="apply", "H3")
indexing_system <- sparkR.callJStatic(x="com.databricks.labs.mosaic.core.index.IndexSystemID", methodName="getIndexSystem", index_system_id)
mosaic_context <- sparkR.newJObject(x="com.databricks.labs.mosaic.functions.MosaicContext", indexing_system, geometry_api)
functions <- sparkR.callJMethod(mosaic_context, "functions")

setwd("/Users/robert.whiffin/Documents/mosaic/R/SparkR")
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
SparkR::collect(sdf)

SparkR::collect(
  SparkR::withColumn(sdf, "wkt", st_aswkt(column("points")))
  )

SparkR::collect(
  SparkR::withColumn(sdf, "length", st_length(st_aswkt(column("points"))))
)

