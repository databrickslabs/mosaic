package com.databricks.labs.mosaic.core.raster.operator.merge

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALWarp
import com.databricks.labs.mosaic.utils.PathUtils

object MergeRasters {

    def merge(rasters: Seq[MosaicRaster]): MosaicRaster = {
        val rasterUUID = java.util.UUID.randomUUID.toString
        val outShortName = rasters.head.getRaster.GetDriver.getShortName

        val rasterPath = PathUtils.createTmpFilePath(rasterUUID, "tif")

        val result2 = GDALWarp.executeWarp(
          rasterPath,
          isTemp = true,
          rasters,
          command = s"gdalwarp -r bilinear -of $outShortName -co COMPRESS=PACKBITS -overwrite"
        )

        result2
    }

}
