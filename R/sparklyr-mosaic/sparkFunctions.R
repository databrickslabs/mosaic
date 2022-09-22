# sparklyr doesnt have this, easier to bring bring it across than use a udf
to_json <- function(sc){
  sparklyr::invoke_static("org.apache.spark.sql.functions", "to_json", spark_session(sc))
}
