# sparklyr approach to getting a jar into a spark session
spark_dependencies <- function(spark_version, scala_version, ...) {
  mosaic_jar = "/Users/robert.whiffin/Downloads/artefacts_2/mosaic-0.1.1-SNAPSHOT-jar-with-dependencies.jar"
  spark_dependency(
    jars = c(
      system.file(
        sprintf(mosaic_jar, spark_version, scala_version), 
        package = "mosaic"
      )
    )
  )
}

.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}