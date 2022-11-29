# sparklyr doesnt have this, easier to bring bring it across than use a udf

#' @description provides an interface to Spark's to_json() function.
#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @param sc sparkContext
#' @name to_json
#' @rdname to_json
#' @return None
#' @export to_json
#' @examples
#' \dontrun{
#' sparklyr_data_frame %>%
#' mutate(spark_json_data = to_json(json_as_string))
#' }
to_json <- function(sc){
  sparklyr::invoke_static("org.apache.spark.sql.functions", "to_json", spark_session(sc))
}
