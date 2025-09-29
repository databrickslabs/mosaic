package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDAL
import com.databricks.labs.gbx.rasterx.operator.{GDALBuildVRT, GDALTranslate}
import org.gdal.gdal.{Dataset, gdal}

/** MergeRasters is a helper object for merging rasters. */
object MergeRasters {

    /**
      * Merges the rasters into a single raster.
      *
      * @param dss
      *   The rasters to merge.
      * @return
      *   A Raster object.
      */
    def merge(dss: Array[Dataset], options: Map[String, String]): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val driver = dss.head.GetDriver()
        val extension = GDAL.getExtension(driver.getShortName)
        val vrtPath = s"/vsimem/merge_rasters_$uuid.vrt"
        val rasterPath = s"/vsimem/merge_rasters_$uuid.$extension"

        val (vrtRaster, vrtOptions) = GDALBuildVRT.executeVRT(
          vrtPath,
          dss,
          options,
          command = s"gdalbuildvrt -resolution highest"
        )

        val result = GDALTranslate.executeTranslate(
          rasterPath,
          vrtRaster,
          command = s"gdal_translate",
          vrtOptions
        )

        vrtRaster.delete()
        gdal.Unlink(vrtPath)

        result
    }

}
