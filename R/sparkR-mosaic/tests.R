library(testthat)

spark_location <- "/usr/spark-download/unzipped/spark-3.4.0-bin-hadoop3"
Sys.setenv(SPARK_HOME = spark_location)
library(SparkR, lib.loc = c(file.path(spark_location, "R", "lib")))
.libPaths(c(file.path(spark_location, "R", "lib"), .libPaths()))

# find the sparkrMosaic tar
file_list <- list.files()
package_file <- file_list[grep(".tar.gz", file_list, fixed=T)]

install.packages(package_file, repos=NULL)
library(sparkrMosaic)

# find the mosaic jar in staging
staging_dir <- "/home/runner/work/mosaic/mosaic/staging/"
mosaic_jar <- list.files(staging_dir)
mosaic_jar <- mosaic_jar[grep("jar-with-dependencies.jar", mosaic_jar, fixed=T)]
print("Looking for mosaic jar in")
mosaic_jar_path <- paste0(staging_dir, mosaic_jar)
print(mosaic_jar_path)

spark <- sparkR.session(
  master = "local[*]"
  ,sparkJars = mosaic_jar_path
)

enableMosaic()

testthat::test_local(path="./sparkrMosaic")