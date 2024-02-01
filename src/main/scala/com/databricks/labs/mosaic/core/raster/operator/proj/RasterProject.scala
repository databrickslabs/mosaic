package com.databricks.labs.mosaic.core.raster.operator.proj

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALWarp
import com.databricks.labs.mosaic.utils.PathUtils
import org.gdal.osr.SpatialReference

/**
  * RasterProject is an object that defines the interface for projecting a
  * raster.
  */
object RasterProject {

    /**
      * Projects a raster to a new CRS. The method handles all the abstractions
      * over GDAL Warp. It uses cubic resampling to ensure that the output is
      * smooth.
      *
      * @param raster
      *   The raster to project.
      * @param destCRS
      *   The destination CRS.
      * @return
      *   A projected raster.
      */
    def project(raster: MosaicRasterGDAL, destCRS: SpatialReference): MosaicRasterGDAL = {
        val outShortName = raster.getDriversShortName

        val resultFileName = PathUtils.createTmpFilePath(GDAL.getExtension(outShortName))

        // Note that Null is the right value here
        val authName = destCRS.GetAuthorityName(null)
        val authCode = destCRS.GetAuthorityCode(null)
        
        val result = GDALWarp.executeWarp(
          resultFileName,
          Seq(raster),
          command = s"gdalwarp -t_srs $authName:$authCode"
        )

        result
    }

}
