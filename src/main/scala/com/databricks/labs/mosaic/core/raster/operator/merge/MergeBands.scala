package com.databricks.labs.mosaic.core.raster.operator.merge

import com.databricks.labs.mosaic.core.raster.gdal.RasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.{GDALBuildVRT, GDALTranslate}
import com.databricks.labs.mosaic.functions.ExprConfig
import com.databricks.labs.mosaic.utils.PathUtils
import org.apache.spark.sql.types.{BinaryType, DataType}

/** MergeBands is a helper object for merging raster bands. */
object MergeBands extends {

    val tileDataType: DataType = BinaryType

    /**
      * Merges the raster bands into a single raster.
      *
      * @param rasters
      *   The rasters to merge.
      * @param resampling
      *   The resampling method to use.
      * @param exprConfigOpt
      *   Option [[ExprConfig]]
      * @return
      *   A MosaicRaster object.
      */
    def merge(rasters: Seq[RasterGDAL], resampling: String, exprConfigOpt: Option[ExprConfig]): RasterGDAL = {
        val outOptions = rasters.head.getWriteOptions

        val vrtPath = PathUtils.createTmpFilePath("vrt", exprConfigOpt)
        val rasterPath = PathUtils.createTmpFilePath(outOptions.extension, exprConfigOpt)

        val vrtRaster = GDALBuildVRT.executeVRT(
            vrtPath,
            rasters,
            command = s"gdalbuildvrt -separate -resolution highest",
            exprConfigOpt
        )

        val result = GDALTranslate.executeTranslate(
            rasterPath,
            vrtRaster,
            command = s"gdal_translate -r $resampling",
            outOptions,
            exprConfigOpt
        )

        vrtRaster.flushAndDestroy()

        result
    }

    /**
      * Merges the raster bands into a single raster. This method allows for
      * custom pixel sizes.
      *
      * @param rasters
      *   The rasters to merge.
      * @param pixel
      *   The pixel size to use.
      * @param resampling
      *   The resampling method to use.
      * @param exprConfigOpt
      *   Option [[ExprConfig]]
      * @return
      *   A MosaicRaster object.
      */
    def merge(rasters: Seq[RasterGDAL], pixel: (Double, Double), resampling: String, exprConfigOpt: Option[ExprConfig]): RasterGDAL = {
        val outOptions = rasters.head.getWriteOptions

        val vrtPath = PathUtils.createTmpFilePath("vrt", exprConfigOpt)
        val rasterPath = PathUtils.createTmpFilePath(outOptions.extension, exprConfigOpt)

        val vrtRaster = GDALBuildVRT.executeVRT(
            vrtPath,
            rasters,
            command = s"gdalbuildvrt -separate -resolution user -tr ${pixel._1} ${pixel._2}",
            exprConfigOpt
        )

        val result = GDALTranslate.executeTranslate(
            rasterPath,
            vrtRaster,
            command = s"gdalwarp -r $resampling",
            outOptions,
            exprConfigOpt
        )

        vrtRaster.flushAndDestroy()

        result
    }

}
