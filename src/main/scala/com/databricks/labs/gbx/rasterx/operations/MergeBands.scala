package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDAL
import com.databricks.labs.gbx.rasterx.operator.{GDALBuildVRT, GDALTranslate}
import org.gdal.gdal.{Dataset, gdal}

/** MergeBands is a helper object for merging raster bands. */
object MergeBands {

    /**
      * Merges the raster bands into a single raster.
      *
      * @param dss
      *   The rasters to merge.
      * @param resampling
      *   The resampling method to use.
      * @return
      *   A Raster object.
      */
    def merge(dss: Seq[Dataset], options: Map[String, String], resampling: String): (Dataset, Map[String, String]) = {
        val uuid1 = java.util.UUID.randomUUID().toString.replace("-", "_")
        val outShortName = dss.head.GetDriver().getShortName
        val extension = GDAL.getExtension(outShortName)

        val vrtPath = s"/vsimem/merge_bands_vrt_$uuid1.vrt"
        val rasterPath = s"/vsimem/merge_bands_$uuid1.$extension"

        val (vrtRaster, vrtOptions) = GDALBuildVRT.executeVRT(
          vrtPath,
          dss.toArray,
          options,
          command = s"gdalbuildvrt -separate -resolution highest"
        )

        val result = GDALTranslate.executeTranslate(
          rasterPath,
          vrtRaster,
          command = s"gdal_translate -r $resampling",
          vrtOptions
        )

        vrtRaster.delete()
        gdal.Unlink(vrtPath)

        result
    }

    /**
      * Merges the raster bands into a single raster. This method allows for
      * custom pixel sizes.
      *
      * @param dss
      *   The rasters to merge.
      * @param pixel
      *   The pixel size to use.
      * @param resampling
      *   The resampling method to use.
      * @return
      *   A Raster object.
      */
    def merge(
        dss: Seq[Dataset],
        options: Map[String, String],
        pixel: (Double, Double),
        resampling: String
    ): (Dataset, Map[String, String]) = {
        val uuid1 = java.util.UUID.randomUUID().toString.replace("-", "_")
        val outShortName = dss.head.GetDriver().getShortName
        val extension = GDAL.getExtension(outShortName)

        val vrtPath = s"/vsimem/merge_bands_vrt_$uuid1.vrt"
        val rasterPath = s"/vsimem/merge_bands_$uuid1.$extension"

        val (vrtRaster, vrtOptions) = GDALBuildVRT.executeVRT(
          vrtPath,
          dss.toArray,
          options,
          command = s"gdalbuildvrt -separate -resolution user -tr ${pixel._1} ${pixel._2}"
        )

        val result = GDALTranslate.executeTranslate(
          rasterPath,
          vrtRaster,
          command = s"gdalwarp -r $resampling",
          vrtOptions
        )

        vrtRaster.delete()
        gdal.Unlink(vrtPath)

        result
    }

}
