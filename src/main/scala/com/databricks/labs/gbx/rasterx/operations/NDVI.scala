package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDAL
import com.databricks.labs.gbx.rasterx.operator.GDALCalc
import com.databricks.labs.gbx.util.NodeFilePathUtil
import org.gdal.gdal.Dataset

/** NDVI is a helper object for computing NDVI. */
object NDVI {

    /**
      * Computes NDVI from a Raster.
      *
      * @param ds
      *   Raster to compute NDVI from.
      * @param redIndex
      *   Index of the red band.
      * @param nirIndex
      *   Index of the near-infrared band.
      * @return
      *   Raster with NDVI computed.
      */
    def compute(ds: Dataset, options: Map[String, String], redIndex: Int, nirIndex: Int): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val driver = ds.GetDriver
        val extension = GDAL.getExtension(driver.getShortName)
        val ndviPath = s"${NodeFilePathUtil.rootPath}/ndvi_$uuid.$extension" // s"/vsimem/ndvi_$uuid.$extension"
        val inPath = ds.GetDescription()
        // noinspection ScalaStyle
        val gdalCalcCommand =
            s"""gdal_calc -A $inPath --A_band=$redIndex -B $inPath --B_band=$nirIndex --outfile=$ndviPath --calc="(B-A)/(B+A)""""

        GDALCalc.executeCalc(gdalCalcCommand, ndviPath, options, ds)
    }

}
