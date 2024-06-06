package com.databricks.labs.mosaic.core.raster.operator.merge

import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.{GDALBuildVRT, GDALTranslate}
import com.databricks.labs.mosaic.expressions.raster.base.RasterPathAware
import com.databricks.labs.mosaic.utils.PathUtils
import org.apache.spark.sql.types.{BinaryType, DataType}

/** MergeRasters is a helper object for merging rasters. */
object MergeRasters extends RasterPathAware {

    val tileDataType: DataType = BinaryType

    /**
      * Merges the rasters into a single raster.
      *
      * @param rasters
      *   The rasters to merge.
      * @param manualMode
      *   Skip deletion of interim file writes, if any.
      * @return
      *   A MosaicRaster object.
      */
    def merge(rasters: Seq[MosaicRasterGDAL], manualMode: Boolean): MosaicRasterGDAL = {
        val outOptions = rasters.head.getWriteOptions

        val vrtPath = PathUtils.createTmpFilePath("vrt")
        val rasterPath = PathUtils.createTmpFilePath(outOptions.extension)

        val vrtRaster = GDALBuildVRT.executeVRT(
          vrtPath,
          rasters,
          command = s"gdalbuildvrt -resolution highest"
        )

        val result = GDALTranslate.executeTranslate(
          rasterPath,
          vrtRaster,
          command = s"gdal_translate",
          outOptions
        )

        pathSafeDispose(vrtRaster, manualMode)

        result
    }

}
