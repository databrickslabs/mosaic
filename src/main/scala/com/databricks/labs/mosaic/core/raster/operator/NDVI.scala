package com.databricks.labs.mosaic.core.raster.operator

import com.databricks.labs.mosaic.core.raster.gdal.RasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALCalc
import com.databricks.labs.mosaic.functions.ExprConfig

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
      * @param exprConfigOpt
      *   Option [[ExprConfig]]
      * @return
      *   MosaicRasterGDAL with NDVI computed.
      */
    def compute(raster: RasterGDAL, redIndex: Int, nirIndex: Int, exprConfigOpt: Option[ExprConfig]): RasterGDAL = {
        val ndviPath = raster.createTmpFileFromDriver(exprConfigOpt)
        // noinspection ScalaStyle
        val gdalCalcCommand =
            s"""gdal_calc -A ${raster.getRawPath} --A_band=$redIndex -B ${raster.getRawPath} --B_band=$nirIndex --outfile=$ndviPath --calc="(B-A)/(B+A)""""

        GDALCalc.executeCalc(gdalCalcCommand, ndviPath, exprConfigOpt)
    }

}
