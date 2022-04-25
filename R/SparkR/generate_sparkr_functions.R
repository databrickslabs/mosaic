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
  paste0(
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
  
}
############################################################

############################################################
get_function_names <- function(scala_file_path){
  #scala_file = file("~/Documents/mosaic/src/main/scala/com/databricks/labs/mosaic/functions/MosaicContext.scala")
  scala_file = file(scala_file_path)
  
  scala_file = readLines(scala_file)
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

  genericFileConn = file("generics.R")
  methodsFileConn = file("functions.R")
  
  generics <- lapply(function_data, function(x){build_generic(parser(x))})
  methods <- lapply(function_data, function(x){build_method(parser(x))})
  writeLines(paste0(generics, collapse="\n"), genericFileConn)
  writeLines(paste0(methods, collapse="\n"), methodsFileConn)
  closeAllConnections()
  
  package.skeleton(
    name="sparkrMosaic"
    ,code_files=c("generics.R", "functions.R","enableMosaic.R")
  )
}

args <- commandArgs(trailingOnly = T)
if (length(args) >  1){
  stop("Please only provide a single argument to generate_sparkr_functions.R")
}
main(args[1])

#if (sys.nframe() == 0){
#  
#  main()
#}#