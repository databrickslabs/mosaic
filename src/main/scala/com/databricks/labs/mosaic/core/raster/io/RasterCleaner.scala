package com.databricks.labs.mosaic.core.raster.io

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.datasource.gdal.ReadAsPath.pathSafeDispose
import com.databricks.labs.mosaic.expressions.raster.base.RasterPathAware
import org.gdal.gdal.Dataset

import scala.util.Try

/** Trait for cleaning up raster objects. */
trait RasterCleaner extends RasterPathAware {

    /**
      * Cleans up the rasters from memory or from temp directory. Cleaning up is
      * destructive and should only be done when the raster is no longer needed.
      * - "safe" means respect debug mode and whether deleting the "path" variable is
      *   allowed and not deleting fuse paths without it specified as allowed and not
      *   deleting if is a parent path.
      * @param aPath
      *   The path to delete if criteria met.
      * @param allowThisPathDelete
      *   Whether to allow the raster "path" to be deleted if the provided path ends up matching
      *   after various normalization.
      * @param manualMode
      *   Skip deletion of interim file writes, if any.
      */
    def safeCleanUpPath(aPath: String, allowThisPathDelete: Boolean, manualMode: Boolean): Unit

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
     * @param tile
     *   The [[MosaicRasterTile]] with the raster.dataset to destroy.
     */
    def destroy(tile: MosaicRasterTile): Unit = {
        Try(destroy(tile.raster))
    }

    /**
      * Flushes the cache and deletes the dataset. Note that this does not
      * unlink virtual files. For that, use gdal.unlink(path).
      *
      * @param raster
      *   The [[MosaicRasterGDAL]] with the dataset to destroy.
      */
    def destroy(raster: MosaicRasterGDAL): Unit = {
        Try(destroy(raster.raster))
    }

    /**
      * Flushes the cache and deletes the dataset. Note that this does not
      * unlink virtual files. For that, use gdal.unlink(path).
      *
      * @param ds
      *   The dataset to destroy.
      */
    def destroy(ds: Dataset): Unit = {
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
      * Destroys the tile's raster JVM object and triggers the managed local raster file deletion.
      *  - destroy is a destructive operation and should only be done when the raster is no longer needed.
      * - `manualMode`` will skip deleting underlying files regardless of `deletePath` value.
 *
      * @param tile
      *   The tile.raster to destroy and clean up.
      * @param manualMode
      *   Skip deletion of interim file writes, if any.
      */
    def dispose(tile: MosaicRasterTile, manualMode: Boolean): Unit = {
        Try(dispose(tile.getRaster, manualMode))
    }

    def dispose(raster: MosaicRasterGDAL, manualMode: Boolean): Unit = {
        pathSafeDispose(raster: MosaicRasterGDAL, manualMode: Boolean)
    }

}
