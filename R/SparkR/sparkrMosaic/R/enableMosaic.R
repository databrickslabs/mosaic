enableMosaic <- function(
  geometryAPI="ESRI"
  ,indexSystem="H3"
  ){
  geometry_api <- sparkR.callJStatic(x="com.databricks.labs.mosaic.core.geometry.api.GeometryAPI", methodName="apply", geometryAPI)
  index_system_id <- sparkR.callJStatic(x="com.databricks.labs.mosaic.core.index.IndexSystemID", methodName="apply", indexSystem)
  indexing_system <- sparkR.callJStatic(x="com.databricks.labs.mosaic.core.index.IndexSystemID", methodName="getIndexSystem", index_system_id)
  mosaic_context <- sparkR.newJObject(x="com.databricks.labs.mosaic.functions.MosaicContext", indexing_system, geometry_api)
  functions <<- sparkR.callJMethod(mosaic_context, "functions")
  
}