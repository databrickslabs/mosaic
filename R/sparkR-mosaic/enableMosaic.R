#' enableMosaic
#' 
#' @description enableMosaic activates the context dependent Databricks Mosaic functions, giving control over the geometry API and index system used.
#' See \url{https://databrickslabs.github.io/mosaic/} for full documentation
#' @param geometryAPI character, default="ESRI"
#' @param indexSystem character, default="H3"
#' @param indexSystem boolean, default=F
#' @name enableMosaic
#' @rdname enableMosaic
#' @return None
#' @export enableMosaic
#' @examples
#' \dontrun{
#' enableMosaic()
#' enableMosaic("ESRI", "H3")
#' enableMosaic("ESRI", "BNG") }
enableMosaic <- function(
  geometryAPI="ESRI"
  ,indexSystem="H3"
  ,rasterAPI="GDAL"
){
  geometry_api <- sparkR.callJStatic(x="com.databricks.labs.mosaic.core.geometry.api.GeometryAPI", methodName="apply", geometryAPI)
  indexing_system <- sparkR.callJStatic(x="com.databricks.labs.mosaic.core.index.IndexSystemFactory", methodName="getIndexSystem", indexSystem)
  
  raster_api <- sparkR.callJStatic(x="com.databricks.labs.mosaic.core.raster.api.RasterAPI", methodName="apply", rasterAPI)
  
  mosaic_context <- sparkR.newJObject(x="com.databricks.labs.mosaic.functions.MosaicContext", indexing_system, geometry_api, raster_api)
  functions <<- sparkR.callJMethod(mosaic_context, "functions")
  # register the sql functions for use in sql() commands
  sparkR.callJMethod(mosaic_context, "register")
  

}