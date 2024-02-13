#' enableGDAL
#' 
#' @description enableGDAL activates GDAL extensions for Mosaic
#' @name enableGDAL
#' @rdname enableGDAL
#' @return None
#' @export enableGDAL
#' @examples
#' \dontrun{
#' enableGDAL() }
enableGDAL <- function(
){
  sparkR.callJStatic(x="com.databricks.labs.mosaic.gdal.MosaicGDAL", methodName="enableGDAL", sparkR.session())
}