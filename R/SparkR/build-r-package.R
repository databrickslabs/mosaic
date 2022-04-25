#Sys.setenv("SPARK_HOME"="/usr/local/Cellar/apache-spark/3.2.1/libexec")
#Sys.setenv("JAVA_HOME"="/usr/local/Cellar/openjdk@8/1.8.0+312/libexec/openjdk.jdk/Contents/Home")
#library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
#spark = sparkR.session(
#  master = "local[*]"
#  ,sparkJars = "/Users/robert.whiffin/Downloads/artefacts/mosaic-0.1.0-SNAPSHOT-jar-with-dependencies.jar"
#  )


# Update the auto-generated code files 
setwd("/Users/robert.whiffin/Documents/mosaic/R/SparkR")
system("Rscript --vanilla generate_sparkr_functions.R /Users/robert.whiffin/Documents/mosaic/src/main/scala/com/databricks/labs/mosaic/functions/MosaicContext.scala")

# Increment the version number in the DESCRIPTION.TEMPLATE and copy into the package folder

# 



#system("R CMD build sparkrMosaic")
#library(devtools)
#devtools::build("sparkrMosaic")
#
#geometry_api <- sparkR.callJStatic(x="com.databricks.labs.mosaic.core.geometry.api.GeometryAPI", methodName="apply", "ESRI")
#index_system_id <- sparkR.callJStatic(x="com.databricks.labs.mosaic.core.index.IndexSystemID", methodName="apply", "H3")
#indexing_system <- sparkR.callJStatic(x="com.databricks.labs.mosaic.core.index.IndexSystemID", methodName="getIndexSystem", index_system_id)
#mosaic_context <- sparkR.newJObject(x="com.databricks.labs.mosaic.functions.MosaicContext", indexing_system, geometry_api)
#functions <- sparkR.callJMethod(mosaic_context, "functions")


