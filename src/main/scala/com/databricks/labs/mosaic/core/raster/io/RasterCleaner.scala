package com.databricks.labs.mosaic.core.raster.io

import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.utils.PathUtils
import org.gdal.gdal.{Dataset, gdal}

import scala.util.Try

trait RasterCleaner {

    /**
     * Destroys the raster object.
     * - rasters can be recreated from file system
     *   path or from content bytes after destroy.
     */
    def destroy(): Unit

}

/** singleton */
object RasterCleaner {

    /**
     * Destroy the tiles raster.
     *
     * @param tile
     *   The [[MosaicRasterTile]] with the raster to destroy.
     */
    def destroy(tile: MosaicRasterTile): Unit = {
        Try(tile.raster.destroy())
    }

    /**
     * Flushes the cache and deletes the JVM object.
     *
     * @param raster
     *   The [[MosaicRasterGDAL]] with the dataset to destroy.
     */
    def destroy(raster: MosaicRasterGDAL): Unit = {
        Try(raster.destroy())
    }

    /**
     * Flushes the cache and deletes the dataset.
     * - not a physical deletion, just the JVM object is deleted.
     * - does not unlink virtual files. For that, use gdal.unlink(path).
     *
     * @param ds
     *   The [[Dataset]] to destroy.
     */
    def destroy(ds: Dataset): Unit = {
        if (ds != null) {
            try {
                ds.FlushCache()
                // Not to be confused with physical deletion
                // - this is just deletes JVM object
                ds.delete()
            } catch {
                case _: Any => ()
            }
        }
    }

    def isSameAsRasterPath(aPath: String, raster: MosaicRasterGDAL): Boolean = {
        PathUtils.getCleanPath(raster.getPath) == PathUtils.getCleanPath(aPath)
    }

    def isSameAsRasterParentPath(aPath: String, raster: MosaicRasterGDAL): Boolean = {
        PathUtils.getCleanPath(raster.getParentPath) == PathUtils.getCleanPath(aPath)
    }

    /**
     * Cleans up the raster driver and references, see [[RasterCleaner]].
     * - This will not clean up a file stored in a Databricks location,
     *   meaning DBFS, Volumes, or Workspace paths are skipped.
     * Unlinks the raster file. After this operation the raster object is no
     * longer usable. To be used as last step in expression after writing to
     * bytes.
     */
    @deprecated("0.4.3 recommend to let CleanUpManager handle")
    def safeCleanUpPath(aPath: String, raster: MosaicRasterGDAL, allowThisPathDelete: Boolean): Unit = {
        // 0.4.2 - don't delete any fuse locations.
        if (
            !PathUtils.isFuseLocation(aPath) && !isSameAsRasterParentPath(aPath, raster)
                && (!isSameAsRasterPath(aPath, raster) || allowThisPathDelete)
        ) {
            Try(gdal.GetDriverByName(raster.getDriversShortName).Delete(aPath))
            PathUtils.cleanUpPath(aPath)
        }
    }
}
