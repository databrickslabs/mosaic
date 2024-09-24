options(warn = -1)
library(testthat)
library(readr)

spark_location <- Sys.getenv("SPARK_HOME")
library(SparkR, lib.loc = c(file.path(spark_location, "R", "lib")))
.libPaths(c(file.path(spark_location, "R", "lib"), .libPaths()))

# find the sparkrMosaic tar
file_list <- list.files()
package_file <- file_list[grep(".tar.gz", file_list, fixed=T)]

install.packages(package_file, repos=NULL)
library(sparkrMosaic)

# find the mosaic jar in staging
staging_dir <- Sys.getenv("MOSAIC_LIB_PATH", "/home/runner/work/mosaic/mosaic/staging/")
mosaic_jar <- list.files(staging_dir)
mosaic_jar <- mosaic_jar[grep("jar-with-dependencies.jar", mosaic_jar, fixed=T)]
print("Looking for mosaic jar in")
mosaic_jar_path <- paste0(staging_dir, mosaic_jar)
print(mosaic_jar_path)

pwd <- getwd()
spark <- sparkR.session(
  master = "local[*]"
  ,sparkJars = mosaic_jar_path,
  sparkConfig = list(
    spark.databricks.labs.mosaic.raster.tmp.prefix = paste0(pwd, "/mosaic_tmp", sep="")
    ,spark.databricks.labs.mosaic.raster.checkpoint = paste0(pwd, "/mosaic_checkpoint", sep="")
  )
)
enableMosaic()

testthat::test_local(path="./sparkrMosaic")