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
    '#\' @rdname ', function_name, ' 
    setGeneric(
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
    else if(x[2] == 'Double'){
      x[2] = "numeric"
    }
    x
  }
  # convert scala type to R types
  args = lapply(input$args, argument_parser)
  # take a copy for building the docs
  param_args = args
  # wrap the strings in speech marks
  args = lapply(args, function(x){c(x[1], paste0("'", x[2], "'"))})
  # collapse down to a single string
  args = lapply(args, function(x){paste0(x,  collapse= ' = ')})
  column_specifiers <- build_column_specifiers(input)
  docstring <- paste0(
    c(paste0(c("#'", function_name), collapse=" "),
      "\n#' See \\url{https://databrickslabs.github.io/mosaic/} for full documentation",
      paste0(c("#' @rdname", function_name), collapse=" "),
      paste0(c("#' @exportMethod", function_name), collapse=" "),
      paste0(sapply(sapply(param_args, function(x){paste(x, collapse=" ")}), function(x){paste("#' @param ", x)}) , collapse="\n") 
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
get_function_names <- function(scala_file_path){
  #scala_file_path = "~/Documents/mosaic/src/main/scala/com/databricks/labs/mosaic/functions/MosaicContext.scala"
  scala_file_object = file(scala_file_path)
  
  scala_file = readLines(scala_file_object)
  closeAllConnections()
  # find where the methods start
  start_string = "    object functions extends Serializable {"
  start_index = grep(start_string, scala_file, fixed=T) + 1
  # find the methods end - will be the next curly bracket
  # need to find where the matching end brace for the start string is located.
  # counter starts at 1 as the start string includes the opening brace
  brace_counter = 1

  for(i in start_index : length(scala_file)){
    # split the string into characters - returns a list so unlist it
    line_characters <- unlist(strsplit(scala_file[i], ''))
    # count the number of brace opens
    n_opens = sum(grepl("{", line_characters, fixed=T))
    # count the number of brace closes
    n_closes = sum(grepl("}", line_characters, fixed=T))
    # update the counter
    brace_counter <- brace_counter + n_opens - n_closes
    if (brace_counter == 0) break

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
  sorted(methods_to_bind, T)
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
  
  genericFileConn <- file("sparkrMosaic/R/generics.R")
  functionsFileConn <- file("sparkrMosaic/R/functions.R")
  
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
