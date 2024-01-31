library(testthat)

if(length(getOption("repos")) < 1) {
  options(repos = c(
    CRAN = "https://cloud.r-project.org"
  ))
}

install.packages("sparklyr", repos="")
library(sparklyr)

spark_home <- Sys.getenv("SPARK_HOME")
spark_home_set(spark_home)
install.packages("sparklyrMosaic_0.4.0.tar.gz", repos = NULL)
library(sparklyrMosaic)

# find the mosaic jar in staging
staging_dir <- Sys.getenv("MOSAIC_LIB_PATH", "/home/runner/work/mosaic/mosaic/staging/")
mosaic_jar <- list.files(staging_dir)
mosaic_jar <- mosaic_jar[grep("jar-with-dependencies.jar", mosaic_jar, fixed=T)]
mosaic_jar_path <- paste0(staging_dir, mosaic_jar)
print(paste("Looking for mosaic jar in", mosaic_jar_path))

config <- sparklyr::spark_config()
config$`sparklyr.jars.default` <- c(mosaic_jar_path)

sc <- spark_connect(master="local[*]", config=config)
enableMosaic(sc)
enableGDAL(sc)

testthat::test_local(path="./sparklyrMosaic")