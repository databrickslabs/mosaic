package com.databricks.labs.mosaic.core.raster.operator.proj

import com.databricks.labs.mosaic.core.raster.gdal.RasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALWarp
import com.databricks.labs.mosaic.functions.ExprConfig
import org.gdal.osr.SpatialReference

/**
  * RasterProject is an object that defines the interface for projecting a
  * tile.
  */
object RasterProject {

    /**
      * Projects a tile to a new CRS. The method handles all the abstractions
      * over GDAL Warp. It uses cubic resampling to ensure that the output is
      * smooth.
      *
      * @param raster
      *   The tile to project.
      * @param destCRS
      *   The destination CRS.
      * @param exprConfigOpt
      *   Option [[ExprConfig]]
      * @return
      *   A projected tile.
      */
    def project(raster: RasterGDAL, destCRS: SpatialReference, exprConfigOpt: Option[ExprConfig]): RasterGDAL = {
        val tmpPath = raster.createTmpFileFromDriver(exprConfigOpt)

        // Note that Null is the right value here
        val authName = destCRS.GetAuthorityName(null)
        val authCode = destCRS.GetAuthorityCode(null)

        val result = GDALWarp.executeWarp(
            tmpPath,
            Seq(raster),
            command = s"gdalwarp -t_srs $authName:$authCode",
            exprConfigOpt
        )

        result
    }

}
