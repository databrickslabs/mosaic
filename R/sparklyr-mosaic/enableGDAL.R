#' enableGDAL
#' 
#' @description enableGDAL activates GDAL extensions for Mosaic
#' @param sc sparkContext
#' @name enableGDAL
#' @rdname enableGDAL
#' @return None
#' @export enableGDAL
#' @examples
#' \dontrun{
#' enableGDAL(sc)}

enableGDAL <- function(
    sc
){
  sparklyr::invoke_static(sc, class="com.databricks.labs.mosaic.gdal.MosaicGDAL", method="enableGDAL", spark_session(sc))
}
