package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDAL
import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import org.gdal.gdal.{Dataset, gdal}

/** ReTile is a helper object for retiling rasters. */
object ReTile {

    def generateWindows(
        ds: Dataset,
        tileWidth: Int,
        tileHeight: Int
    ): Seq[(Int, Int, Int, Int)] = {
        val xR = ds.getRasterXSize
        val yR = ds.getRasterYSize
        val xTiles = (xR + tileWidth  - 1) / tileWidth
        val yTiles = (yR + tileHeight - 1) / tileHeight
        for {
            y <- 0 until yTiles
            x <- 0 until xTiles
        } yield {
            val xMin = x * tileWidth
            val yMin = y * tileHeight
            val xOff = math.min(tileWidth,  xR - xMin)
            val yOff = math.min(tileHeight, yR - yMin)
            (xMin, yMin, xOff, yOff)
        }
    }

    def getTile(
        ds: Dataset,
        options: Map[String, String],
        xStart: Int,
        yStart: Int,
        xOffset: Int,
        yOffset: Int
    ): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val driver = ds.GetDriver
        val extension = GDAL.getExtension(driver.getShortName)

        val rasterPath = s"/vsimem/retile_$uuid.$extension"

        val result = GDALTranslate.executeTranslate(
          rasterPath,
          ds,
          command = s"gdal_translate -srcwin $xStart $yStart $xOffset $yOffset",
          options
        )

        val isEmpty = RasterAccessors.isEmpty(result._1)

        if (isEmpty) {
            result._1.delete()
            gdal.Unlink(rasterPath)
            null
        } else {
            (result._1, result._2)
        }
    }

    /**
      * Retiles a raster into tiles. Empty tiles are discarded. The tile size is
      * specified by the user via the tileWidth and tileHeight parameters.
      *
      * @param ds
      *   The raster to retile.
      * @param tileWidth
      *   The width of the tiles.
      * @param tileHeight
      *   The height of the tiles.
      * @return
      *   A sequence of Raster objects.
      */
    def reTile(
        ds: Dataset,
        options: Map[String, String],
        tileWidth: Int,
        tileHeight: Int
    ): Seq[(Dataset, Map[String, String])] = reTileIter(ds, options, tileWidth, tileHeight).toSeq

    def reTileIter(
        ds: Dataset,
        options: Map[String, String],
        tileWidth: Int,
        tileHeight: Int
    ): Iterator[(Dataset, Map[String, String])] = {
        val windows = generateWindows(ds, tileWidth, tileHeight)
        reTileIter(ds, options, windows)
    }

    def reTileIter(
        ds: Dataset,
        options: Map[String, String],
        windows: Seq[(Int, Int, Int, Int)]
    ): Iterator[(Dataset, Map[String, String])] with AutoCloseable = {
        new Iterator[(Dataset, Map[String, String])] with AutoCloseable {
            private var _ds = ds
            private var i = 0
            private var fetched = false
            private var closed = false
            private var nextTile: (Dataset, Map[String, String]) = _

            private def advance(): Unit = {
                fetched = true
                nextTile = null
                while (i < windows.length && nextTile == null) {
                    val (xs, ys, xo, yo) = windows(i)
                    i += 1
                    nextTile = getTile(_ds, options, xs, ys, xo, yo) // returns null if empty
                }
                if (i >= windows.length && nextTile == null) close()
            }

            override def hasNext: Boolean = {
                if (!fetched && !closed) advance()
                !closed && nextTile != null
            }

            override def next(): (Dataset, Map[String, String]) = {
                if (!fetched && !closed) advance()
                fetched = false
                nextTile
            }

            override def close(): Unit = {
                if (!closed) {
                    closed = true
                    RasterAccessors.unlink(_ds)
                    _ds = null
                }
            }

        }
    }

}
