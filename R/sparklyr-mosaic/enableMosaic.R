#' enableMosaic
#' 
#' @description enableMosaic activates the context dependent Databricks Mosaic functions, giving control over the geometry API and index system used.
#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @param sc sparkContext
#' @param geometryAPI character, default="ESRI"
#' @param indexSystem character, default="H3"
#' @name enableMosaic
#' @rdname enableMosaic
#' @return None
#' @export enableMosaic
#' @examples
#' \dontrun{
#' enableMosaic()
#' enableMosaic("ESRI", "H3")
#' enableMosaic("ESRI", "BNG")}

enableMosaic <- function(
    sc
    ,geometryAPI="ESRI"
    ,indexSystem="H3"
    ,rasterAPI="GDAL"
){
  
  geometry_api <- sparklyr::invoke_static(sc, class="com.databricks.labs.mosaic.core.geometry.api.GeometryAPI", method="apply", geometryAPI)
  index_system_id <- sparklyr::invoke_static(sc, class="com.databricks.labs.mosaic.core.index.IndexSystemID", method="apply", indexSystem)
  raster_api <- sparklyr::invoke_static(sc, class="com.databricks.labs.mosaic.core.raster.api.RasterAPI", method="apply", rasterAPI)
  indexing_system <- sparklyr::invoke_static(sc, class="com.databricks.labs.mosaic.core.index.IndexSystemID", method="getIndexSystem", index_system_id)
  mosaic_context <- sparklyr::invoke_new(sc, class="com.databricks.labs.mosaic.functions.MosaicContext", indexing_system, geometry_api, raster_api)
  functions <<- sparklyr::invoke(mosaic_context, "functions")
  sparklyr::invoke(mosaic_context, "register")
  
}
