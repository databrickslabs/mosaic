package com.databricks.labs.mosaic.core.raster.io

import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import org.gdal.gdal.Dataset

/** Trait for cleaning up raster objects. */
trait RasterCleaner {

    /**
      * Cleans up the rasters from memory or from temp directory. Cleaning up is
      * destructive and should only be done when the raster is no longer needed.
      */
    def cleanUp(): Unit

    /**
      * Destroys the raster object. Rasters can be recreated from file system
      * path or from content bytes after destroy.
      */
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
    def destroy(ds: => Dataset): Unit = {
        if (ds != null) {
            try {
                ds.FlushCache()
                // Not to be confused with physical deletion, this is just deletes jvm object
                ds.delete()
            } catch {
                case _: Any => ()
            }
        }
    }

    /**
      * Destroys and cleans up the raster object. This is a destructive operation and should
      * only be done when the raster is no longer needed.
      *
      * @param raster
      *   The raster to destroy and clean up.
      */
    def dispose(raster: => Any): Unit = {
        raster match {
            case r: MosaicRasterGDAL  =>
                    r.destroy()
                    r.cleanUp()
            case rt: MosaicRasterTile =>
                    rt.getRaster.destroy()
                    rt.getRaster.cleanUp()
            // NOOP for simpler code handling in expressions, removes need for repeated if/else
            case _                    => ()
        }
    }

}
