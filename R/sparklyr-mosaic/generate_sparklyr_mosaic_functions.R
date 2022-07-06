############################################################
build_mosaic_function <- function(input){
  function_name = input$function_name
  paste0(
    '#\' @rdname ', function_name,'\n',
    sprintf(
      '%s <- function(sc){
  sparklyr::invoke(functions, "%s", spark_session(sc))
}', function_name, function_name
      )
    )
}

x = list()
x$function_name = "st_aswkt"
build_mosaic_function(x)
scala_file_path <- "../../src/main/scala/com/databricks/labs/mosaic/functions/MosaicContext.scala"

function_data = get_function_names(scala_file_path)
parsed <- lapply(function_data, parser)



functions <- lapply(parsed, build_mosaic_function)

functionsFileConn <- file("sparklyr-functions.R")

writeLines(paste0(functions, collapse="\n"), functionsFileConn)
closeAllConnections()

# copy enableMosaic and column to directory
file.copy("enableMosaic.R", "sparkrMosaic/R/enableMosaic.R", overwrite=T)
