repos = c(
  "https://cran.ma.imperial.ac.uk" = "https://cran.ma.imperial.ac.uk"
  ,"https://www.stats.bris.ac.uk/R" = "https://www.stats.bris.ac.uk/R"
  ,"https://cran.rstudio.com/"  = "https://cran.rstudio.com/" 
)

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

  # build doc
  devtools::document("sparkrMosaic")

  ## build package
  devtools::build("sparkrMosaic")
  
}


build_sparkr_mosaic()
