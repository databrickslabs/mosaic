repos = c(
    CRAN = "https://packagemanager.posit.co/cran/__linux__/focal/latest"
   ,"https://cran.ma.imperial.ac.uk" = "https://cran.ma.imperial.ac.uk"
  ,"https://www.stats.bris.ac.uk/R" = "https://www.stats.bris.ac.uk/R"
  ,"https://cran.rstudio.com/"  = "https://cran.rstudio.com/" 
)

options(HTTPUserAgent = sprintf("R/%s R (%s)", getRversion(), paste(getRversion(), R.version["platform"], R.version["arch"], R.version["os"])))
options(download.file.extra = sprintf("--header \"User-Agent: R (%s)\"", paste(getRversion(), R.version["platform"], R.version["arch"], R.version["os"])))

print(sprintf("R/%s R (%s)", getRversion(), paste(getRversion(), R.version["platform"], R.version["arch"], R.version["os"])))

mirror_is_up <- function(x){
  out <- tryCatch({
    available.packages(contrib.url(x))
  }
  ,error = function(cond){return(0)}
  ,warning = function(cond){return(0)}
  ,finally = function(cond){}
  )
  return(length(out))
}

mirror_status = lapply(repos, mirror_is_up)
for(repo in names(mirror_status)){
  if (mirror_status[[repo]] > 1){
    repo <<- repo
    break
  }
}

.libPaths(c("/usr/lib/R/site-library", .libPaths()))
install.packages("pkgbuild", repos=repo)
install.packages("roxygen2", repos=repo)
install.packages("sparklyr", repos=repo)
spark_location <- "/usr/spark-download/unzipped/spark-3.2.1-bin-hadoop2.7"
Sys.setenv(SPARK_HOME = spark_location)

library(SparkR, lib.loc = c(file.path(spark_location, "R", "lib")))


library(pkgbuild)
library(roxygen2)
library(sparklyr)



build_mosaic_bindings <- function(){
  # build functions
  scala_file_path <- "../src/main/scala/com/databricks/labs/mosaic/functions/MosaicContext.scala"
  system_cmd <- paste0(c("Rscript --vanilla generate_R_bindings.R", scala_file_path), collapse = " ")
  system(system_cmd)

  # build doc
  roxygen2::roxygenize("sparkR-mosaic/sparkrMosaic")
  roxygen2::roxygenize("sparklyr-mosaic/sparklyrMosaic")
  
  
  ## build package
  pkgbuild::build("sparkR-mosaic/sparkrMosaic")
  pkgbuild::build("sparklyr-mosaic/sparklyrMosaic")
  
}

build_mosaic_bindings()
