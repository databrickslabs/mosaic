package com.databricks.labs.mosaic.core.raster.gdal_raster

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import org.gdal.gdal.Dataset

trait RasterCleaner {

    def cleanUp(): Unit

    def destroy(): Unit

}

object RasterCleaner {

    /**
      * Flushes the cache and deletes the dataset. Note that this does not
      * unlink virtual files. For that, use gdal.unlink(path).
      *
      * @param ds
      *   The dataset to destroy.
      */
    def destroy(ds: Dataset): Unit = {
        if (ds != null) {
            ds.FlushCache()
            // Not to be confused with physical deletion, this is just deletes jvm object
            ds.delete()
        }
    }

    def dispose(raster: Any): Unit = {
        raster match {
            case r: MosaicRaster =>
                r.destroy()
                r.cleanUp()
            // NOOP for simpler code handling in expressions, removes need for repeated if/else
            case _               => ()
        }
    }

}
