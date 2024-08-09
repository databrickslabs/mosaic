package com.databricks.labs.mosaic.core.raster.operator.merge

import com.databricks.labs.mosaic.core.raster.gdal.RasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.{GDALBuildVRT, GDALTranslate}
import com.databricks.labs.mosaic.functions.ExprConfig
import com.databricks.labs.mosaic.utils.PathUtils
import org.apache.spark.sql.types.{BinaryType, DataType}

/** MergeBands is a helper object for merging tile bands. */
object MergeBands {

    val tileDataType: DataType = BinaryType

    /**
      * Merges the tile bands into a single tile.
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

        //scalastyle:off println
        //println(s"MergeBands - merge - rasterPath? $rasterPath")
        //scalastyle:on println

        val vrtRaster = GDALBuildVRT.executeVRT(
            vrtPath,
            rasters,
            command = s"gdalbuildvrt -separate -resolution highest",
            exprConfigOpt
        )

        if (vrtRaster.isEmptyRasterGDAL) {
            vrtRaster
        } else {

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
    }

    /**
      * Merges the tile bands into a single tile. This method allows for
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
