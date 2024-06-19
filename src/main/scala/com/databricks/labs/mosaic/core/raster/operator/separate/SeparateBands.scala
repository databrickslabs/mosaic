package com.databricks.labs.mosaic.core.raster.operator.separate

import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALTranslate
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.utils.PathUtils
import org.apache.spark.sql.types.{DataType, StringType}

/**
  * ReTile is a helper object for splitting multi-band rasters into
  * single-band-per-row.
  * -
  */
object SeparateBands {

    val tileDataType: DataType = StringType // always use checkpoint

    /**
      * Separates raster bands into separate rasters. Empty bands are discarded.
      *
      * @param tile
      *   The raster to retile.
      * @return
      *   A sequence of MosaicRasterTile objects.
      */
    def separate(
        tile: => MosaicRasterTile
    ): Seq[MosaicRasterTile] = {
        val raster = tile.getRaster
        val tiles = for (i <- 0 until raster.numBands) yield {
            val fileExtension = raster.getRasterFileExtension
            val rasterPath = PathUtils.createTmpFilePath(fileExtension)
            val shortDriver = raster.getDriversShortName
            val outOptions = raster.getWriteOptions

            val result = GDALTranslate.executeTranslate(
              rasterPath,
              raster,
              command = s"gdal_translate -of $shortDriver -b ${i + 1}",
              writeOptions = outOptions
            )

            if (!result.isEmpty) {
                // copy to checkpoint dir
                val checkpointPath = result.writeToCheckpointDir(doDestroy = true)
                val newParentPath = result.createInfo("path")
                val bandVal = (i + 1).toString

                result.destroy()

                (
                    true,
                    MosaicRasterGDAL(
                        null,
                        result.createInfo + (
                            "path" -> checkpointPath, "parentPath" -> newParentPath, "bandIndex" -> bandVal),
                        -1
                    )
                )

            } else {
                result.destroy() // destroy inline for performance
                (false, result) // empty result
            }
        }

        val (result, _) = tiles.partition(_._1)
        result.map(t => new MosaicRasterTile(null, t._2, tileDataType))
    }

}
