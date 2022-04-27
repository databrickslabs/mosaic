#!/usr/bin/env Rscript
library(methods)

parser <- function(x){
  #split on left bracket to get name
  splitted = strsplit(x, "(", fixed=T)[[1]]
  # extract function name
  function_name = splitted[1]
  # remove the trailing bracket
  args = gsub( ")", '',splitted[2], fixed=T)
  args = strsplit(args, ", ", fixed=T)[[1]]
  args = lapply(args, function(x){strsplit(x, ": ", fixed=T)}[[1]])
  output = list(
    "function_name" = function_name
    ,"args"=args
  )
  output
}

############################################################
build_generic <- function(input){
  function_name = input$function_name
  args = lapply(input$args, function(x){x[1]})
  paste0(
    'setGeneric(
        name="',function_name,'"
            ,def=function(',paste0(args, collapse=','), ')  {standardGeneric("',function_name, '")}
              )
                  ')
}
############################################################
build_column_specifiers <- function(input){
  args = lapply(input$args, function(x){x[1]})
  build_column_specifier <- function(arg){ 
    return(paste0(arg, '@jc'))
  }
  
  if (length(args) > 1){
    specs <- paste0(unlist(lapply(args, build_column_specifier)), collapse = ", ")
    return(specs)
  }
  else return( build_column_specifier(args))
}
############################################################
build_method<-function(input){
  function_name = input$function_name
  arg_names = lapply(input$args, function(x){c(x[1])})
  #this handles converting non-Column arguments to their R equivalents
  argument_parser <- function(x){
    if(x[2] == 'Int'){
      x[2] = "numeric"
    }
    else if(x[2] == 'String'){
      x[2] = "character"
    }
    x
  }
  args = lapply(input$args, argument_parser)
  args = lapply(args, function(x){c(x[1], paste0("'", x[2], "'"))})
  args = lapply(args, function(x){paste0(x,  collapse= ' = ')})
  column_specifiers <- build_column_specifiers(input)
  docstring <- paste0(
    c(paste0(c("#'", function_name), collapse=" "),
      "#' See \\url{https://databrickslabs.github.io/mosaic/} for full documentation",
      paste0(c("#' @name", function_name), collapse=" "),
      paste0(c("#' @rdname", function_name), collapse=" "),
      paste0(c("#' @exportMethod", function_name), collapse=" ")
    )
    ,collapse="\n"
  )
  function_def <- paste0(
    'setMethod(
              f = "',function_name, '"
              ,signature(
                   ',paste0(args, collapse = "\n                   ,"),
    '
                )
              ,function(', paste0(arg_names, collapse= ','), ') {
                  jc <- sparkR.callJMethod(functions, "', function_name,'", ', column_specifiers, ' )
                  column(jc)
                  }
              )')
  paste0(c(docstring, function_def, "\n\n\n"), collapse="\n")
  
}
############################################################
# write tests

write_tests <- function(input){
  function_name = input$function_name
  arg_names = lapply(input$args, function(x){c(x[1])})
  
  # name of test
  test_name = paste0("test__", function_name)
  # define function signature
  function_signature = paste0(
    "function(spark_df, "
    , paste(arg_names, sep=', ', collapse=", ") ,")" 
  )
  # wrap colnames in "column()"
  wrapped_args <- paste(sapply(arg_names, function(x){paste0("column('", x, "')")}), sep=', ', collapse=", ")
  # function body
  function_body <- paste0(
    "{\n",
    "\tSparkR::withColumn(spark_df, ",
    paste0("'",function_name,"', "),
    function_name,
    "(",
    wrapped_args,
    "))\n}"
  )
  # bring it all together
  full_test <- paste0(test_name, " <- ",function_signature, function_body)
  full_test
}

############################################################
get_function_names <- function(scala_file_path){
  #scala_file_path = "~/Documents/mosaic/src/main/scala/com/databricks/labs/mosaic/functions/MosaicContext.scala"
  scala_file_object = file(scala_file_path)
  
  scala_file = readLines(scala_file_object)
  closeAllConnections()
  # find where the methods start
  start_string = "    object functions extends Serializable {"
  start_index = grep(start_string, scala_file, fixed=T) + 1
  # find the methods end - will be the next curly bracket
  for(i in start_index : length(scala_file)){
    if(grepl("}", scala_file[i], fixed=T)){
      break
    }
  }
  methods_to_bind = scala_file[start_index:i]
  # remove any line that doesn't start with def
  def_mask = grepl("def ", methods_to_bind, fixed = T)
  methods_to_bind = methods_to_bind[def_mask]
  # parse the string to get just the function_name(input:type...) pattern
  methods_to_bind = unlist(lapply(methods_to_bind, function(x){
    substr(x
           , regexpr("def ", x, fixed=T)[1]+4 # get the starting point to account for whitespace
           , regexpr("): ", x, fixed=T)[1] # get the end point of where the return is.
    )
  }
  ))
  methods_to_bind
}

########################################################################
main <- function(scala_file_path){
  
  # this assumes working directoy is the SparkR folder
  #scala_file_path="../../src/main/scala/com/databricks/labs/mosaic/functions/MosaicContext.scala"
  function_data = get_function_names(scala_file_path)
  parsed <- lapply(function_data, parser)
  

  functions_header <- "#' @include generics.R\n\nNULL"
  
  generics <- lapply(parsed, build_generic)
  functions <- lapply(parsed, build_method)
  functions <- append(functions_header, functions)
  #write tests
  #tests <- lapply(parsed, write_tests)
  
  genericFileConn <- file("sparkrMosaic/R/generics.R")
  functionsFileConn <- file("sparkrMosaic/R/functions.R")
  #testsFileConn = file("sparkrMosaic/tests/testthat/tests.R")
  
  writeLines(paste0(generics, collapse="\n"), genericFileConn)
  writeLines(paste0(functions, collapse="\n"), functionsFileConn)
  closeAllConnections()
  
  # copy enableMosaic and column to directory
  file.copy("enableMosaic.R", "sparkrMosaic/R/enableMosaic.R", overwrite=T)
  

}

args <- commandArgs(trailingOnly = T)
if (length(args) !=  1){
  stop("Please provide the MosaicContext.scala file path to generate_sparkr_functions.R")
}
main(args[1])
