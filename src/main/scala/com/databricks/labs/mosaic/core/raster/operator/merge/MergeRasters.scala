package com.databricks.labs.mosaic.core.raster.operator.merge

import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALWarp
import com.databricks.labs.mosaic.utils.PathUtils
import org.gdal.gdal.gdal

object MergeRasters {

    def merge(rasters: Seq[MosaicRasterGDAL]): MosaicRasterGDAL = {
        val rasterUUID = java.util.UUID.randomUUID.toString
        val outShortName = rasters.head.getDriversShortName
        val extension = gdal.GetDriverByName(outShortName).GetMetadataItem("DMD_EXTENSION")

        val rasterPath = PathUtils.createTmpFilePath(rasterUUID, extension)

        val result = GDALWarp.executeWarp(
          rasterPath,
          isTemp = true,
          rasters,
          command = s"gdalwarp -r bilinear -of $outShortName -co COMPRESS=PACKBITS -overwrite"
        )

        result
    }

}
