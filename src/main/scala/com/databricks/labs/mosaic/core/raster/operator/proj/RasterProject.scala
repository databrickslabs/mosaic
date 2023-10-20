package com.databricks.labs.mosaic.core.raster.operator.proj

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALWarp
import com.databricks.labs.mosaic.utils.PathUtils
import org.gdal.osr.SpatialReference

object RasterProject {

    def project(raster: MosaicRasterGDAL, destCRS: SpatialReference): MosaicRasterGDAL = {
        val outShortName = raster.getRaster.GetDriver().getShortName

        val resultFileName = PathUtils.createTmpFilePath(raster.uuid.toString, GDAL.getExtension(outShortName))

        // Note that Null is the right value here
        val authName = destCRS.GetAuthorityName(null)
        val authCode = destCRS.GetAuthorityCode(null)

        val result = GDALWarp.executeWarp(
          resultFileName,
          isTemp = true,
          Seq(raster),
          command = s"gdalwarp -of $outShortName -t_srs $authName:$authCode -r cubic -overwrite -co COMPRESS=PACKBITS"
        )

        result
    }

}
