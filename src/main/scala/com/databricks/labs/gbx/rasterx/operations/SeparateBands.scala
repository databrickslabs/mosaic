package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDAL
import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import org.gdal.gdal.Dataset

/**
  * ReTile is a helper object for splitting multi-band rasters into
  * single-band-per-row.
  */
object SeparateBands {

    def getTile(ds: Dataset, options: Map[String, String], bandIdx: Int): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val driver = ds.GetDriver
        val extension = GDAL.getExtension(driver.getShortName)
        val rasterPath = s"/vsimem/separate_bands_$uuid.$extension"

        val (resDs, resMtd) = GDALTranslate.executeTranslate(
          rasterPath,
          ds,
          command = s"gdal_translate -b ${bandIdx + 1}",
          options
        )

        resDs.SetMetadataItem("RASTERX_BAND_INDEX", (bandIdx + 1).toString)
        resDs.FlushCache()

        (resDs, resMtd)
    }

    /**
      * Separates raster bands into separate rasters. Empty bands are discarded.
      *
      * @param ds
      *   The raster to retile.
      * @return
      *   A sequence of Raster objects.
      */
    def separateIter(
        ds: Dataset,
        options: Map[String, String]
    ): Iterator[(Dataset, Map[String, String])] = {
        val bandCount = ds.GetRasterCount
        new Iterator[(Dataset, Map[String, String])] with AutoCloseable {
            private var currentBand = 0
            private var _ds = ds
            private var closed = false

            override def hasNext: Boolean = {
                val more = currentBand < bandCount
                if (!more) close()
                more
            }

            override def next(): (Dataset, Map[String, String]) = {
                if (!hasNext) return null
                val tile = getTile(ds, options, currentBand)
                currentBand += 1
                if (currentBand >= bandCount) close()
                tile
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
