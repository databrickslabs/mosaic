# repos = c(
#    "https://cran.ma.imperial.ac.uk" = "https://cran.ma.imperial.ac.uk"
#   ,"https://www.stats.bris.ac.uk/R" = "https://www.stats.bris.ac.uk/R"
#   ,"https://cran.rstudio.com/"  = "https://cran.rstudio.com/"
# )
options(repos = c(CRAN = "https://packagemanager.posit.co/cran/__linux__/focal/latest"))
# CRAN = "https://packagemanager.posit.co/cran/__linux__/focal/latest"
#
# options(HTTPUserAgent = sprintf("R/%s R (%s)", getRversion(), paste(getRversion(), R.version["platform"], R.version["arch"], R.version["os"])))
# options(download.file.extra = sprintf("--header \"User-Agent: R (%s)\"", paste(getRversion(), R.version["platform"], R.version["arch"], R.version["os"])))
#
# print(sprintf("R/%s R (%s)", getRversion(), paste(getRversion(), R.version["platform"], R.version["arch"], R.version["os"])))

# mirror_is_up <- function(x){
#   out <- tryCatch({
#     available.packages(contrib.url(x))
#   }
#   ,error = function(cond){return(0)}
#   ,warning = function(cond){return(0)}
#   ,finally = function(cond){}
#   )
#   return(length(out))
# }
#
# mirror_status = lapply(repos, mirror_is_up)
# for(repo in names(mirror_status)){
#   if (mirror_status[[repo]] > 1){
#     repo <<- repo
#     break
#   }
# }

# .libPaths(c("/usr/lib/R/site-library", .libPaths()))

# install.packages("pkgbuild", repos="https://packagemanager.posit.co/cran/__linux__/bullseye/latest")
# install.packages("roxygen2", repos="https://packagemanager.posit.co/cran/__linux__/bullseye/latest")
# install.packages("sparklyr", repos="https://packagemanager.posit.co/cran/__linux__/bullseye/latest")
install.packages("pkgbuild")
install.packages("roxygen2")
install.packages("sparklyr")