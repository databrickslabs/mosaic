package com.databricks.labs.mosaic.core.raster.operator

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALCalc
import com.databricks.labs.mosaic.utils.PathUtils

/** NDVI is a helper object for computing NDVI. */
object NDVI {

    /**
      * Computes NDVI from a MosaicRasterGDAL.
      *
      * @param raster
      *   MosaicRasterGDAL to compute NDVI from.
      * @param redIndex
      *   Index of the red band.
      * @param nirIndex
      *   Index of the near-infrared band.
      * @return
      *   MosaicRasterGDAL with NDVI computed.
      */
    def compute(raster: => MosaicRasterGDAL, redIndex: Int, nirIndex: Int): MosaicRasterGDAL = {
        val tmpPath = PathUtils.createTmpFilePath(raster.uuid.toString, GDAL.getExtension(raster.getDriversShortName))
        raster.writeToPath(tmpPath)
        val tmpRaster = MosaicRasterGDAL(tmpPath, isTemp=true, raster.getParentPath, raster.getDriversShortName, raster.getMemSize)
        val ndviPath = PathUtils.createTmpFilePath(raster.uuid.toString + "NDVI", GDAL.getExtension(raster.getDriversShortName))
        // noinspection ScalaStyle
        val gdalCalcCommand =
            s"""gdal_calc -A ${tmpRaster.getPath} --A_band=$redIndex -B ${tmpRaster.getPath} --B_band=$nirIndex --outfile=$ndviPath --calc="(B-A)/(B+A)""""

        GDALCalc.executeCalc(gdalCalcCommand, ndviPath)
    }

}
