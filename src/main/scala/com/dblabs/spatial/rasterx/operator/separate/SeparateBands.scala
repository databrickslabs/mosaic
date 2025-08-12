package com.dblabs.spatial.rasterx.operator.separate

import com.databricks.labs.mosaic.core.raster.io.RasterCleaner.dispose
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALTranslate
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.utils.PathUtils

import java.nio.file.{Files, Paths}
import scala.util.Try

/**
  * ReTile is a helper object for splitting multi-band rasters into
  * single-band-per-row.
  */
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
            val outOptions = raster.getWriteOptions

            val result = GDALTranslate.executeTranslate(
              rasterPath,
              raster,
              command = s"gdal_translate -b ${i + 1}",
              writeOptions = outOptions
            )

            val isEmpty = result.isEmpty

            result.raster.SetMetadataItem("MOSAIC_BAND_INDEX", (i + 1).toString)
            result.raster.GetDriver().CreateCopy(result.path, result.raster)

            if (isEmpty) dispose(result)

            val size = Try(Files.size(Paths.get(result.path))).getOrElse(-1L)
            (
              isEmpty,
              result.copy(createInfo =
                  result.createInfo ++
                      Map("bandIndex" -> (i + 1).toString, "size" -> size.toString)
              ),
              i
            )

        }

        val (_, valid) = tiles.partition(_._1)

        valid.map(t => new MosaicRasterTile(null, t._2))

    }

}
