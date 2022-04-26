#' S4 class that represents a SparkDataFrame column
#'
#' The column class supports unary, binary operations on SparkDataFrame columns
#'
#' @rdname column
#'
#' @slot jc reference to JVM SparkDataFrame column
#' @note Column since 1.4.0
setClass("Column",
         slots = list(jc = "jobj"))