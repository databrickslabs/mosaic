package com.databricks.labs.mosaic.core.raster.operator.separate

import com.databricks.labs.mosaic.core.raster.io.RasterCleaner.dispose
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALTranslate
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.utils.PathUtils

/** ReTile is a helper object for splitting multi-band rasters into single-band-per-row. */
object SeparateBands {

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

            val result = GDALTranslate.executeTranslate(
              rasterPath,
              raster,
              command = s"gdal_translate -of $shortDriver -b ${i + 1} -co COMPRESS=DEFLATE"
            )

            val isEmpty = result.isEmpty

            result.raster.SetMetadataItem("MOSAIC_BAND_INDEX", (i + 1).toString)
            result.raster.GetDriver().CreateCopy(result.path, result.raster)

            if (isEmpty) dispose(result)

            (isEmpty, result, i)

        }

        val (_, valid) = tiles.partition(_._1)

        valid.map(t => new MosaicRasterTile(null, t._2, raster.getParentPath, raster.getDriversShortName))

    }

}
