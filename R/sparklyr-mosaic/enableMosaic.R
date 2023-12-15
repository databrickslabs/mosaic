#' enableMosaic
#' 
#' @description enableMosaic activates the context dependent Databricks Mosaic functions, giving control over the geometry API and index system used.
#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @param sc sparkContext
#' @param geometryAPI character, default="JTS"
#' @param indexSystem character, default="H3"
#' @name enableMosaic
#' @rdname enableMosaic
#' @return None
#' @export enableMosaic
#' @examples
#' \dontrun{
#' enableMosaic()
#' enableMosaic("JTS", "H3")
#' enableMosaic("JTS", "BNG")}

enableMosaic <- function(
    sc
    ,geometryAPI="JTS"
    ,indexSystem="H3"
){
  
  geometry_api <- sparklyr::invoke_static(sc, class="com.databricks.labs.mosaic.core.geometry.api.GeometryAPI", method="apply", geometryAPI)
  indexing_system <- sparklyr::invoke_static(sc, class="com.databricks.labs.mosaic.core.index.IndexSystemFactory", method="getIndexSystem", indexSystem)
  mosaic_context <- sparklyr::invoke_new(sc, class="com.databricks.labs.mosaic.functions.MosaicContext", indexing_system, geometry_api)
  functions <<- sparklyr::invoke(mosaic_context, "functions")
  sparklyr::invoke(mosaic_context, "register")
  
}
