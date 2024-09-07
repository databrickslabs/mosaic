package com.databricks.labs.mosaic.core.raster.gdal

import com.databricks.labs.mosaic.NO_PATH_STRING
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.io.RasterIO
import com.databricks.labs.mosaic.functions.{ExprConfig, MosaicContext}
import com.databricks.labs.mosaic.utils.{FileUtils, PathUtils, SysUtils}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import java.nio.file.{Files, Paths}
import java.util.UUID
import scala.util.Try

trait GDALWriter {

    /**
     * Writes the given rasters to either a path or a byte array.
     *
     * @param rasters
     *   The rasters to write.
     * @param rasterDT
     *   The type of tile to write.
     *   - if string write to checkpoint
     *   - otherwise, write to bytes
     * @param doDestroy
     *   Whether to destroy the internal object after serializing.
     * @param exprConfigOpt
     *   Option [[ExprConfig]]
     * @return
     *   Returns the paths of the written rasters.
     */
    def writeRasters(
                        rasters: Seq[RasterGDAL],
                        rasterDT: DataType,
                        doDestroy: Boolean,
                        exprConfigOpt: Option[ExprConfig]
                    ): Seq[Any]


    // ///////////////////////////////////////////////////////////////
    // Writers for [[BinaryType]] and [[StringType]]
    // ///////////////////////////////////////////////////////////////

    /**
     * Writes a tile to a byte array.
     * - This is local tmp write, `tile.finalizeRaster` handles fuse.
     *
     * @param raster
     *   The [[RasterGDAL]] object that will be used in the write.
     * @param doDestroy
     *   A boolean indicating if the tile object should be destroyed after
     *   writing.
     *   - file paths handled separately.
     * @param exprConfigOpt
     *   Option [[ExprConfig]]
     * @return
     *   A byte array containing the tile data.
     */
    def writeRasterAsBinaryType(
                                   raster: RasterGDAL,
                                   doDestroy: Boolean,
                                   exprConfigOpt: Option[ExprConfig]
                               ): Array[Byte] = {
        try {
            val tmpDir = MosaicContext.createTmpContextDir(exprConfigOpt)
            val tmpPathOpt = raster.datasetGDAL.datasetOrPathCopy(tmpDir, doDestroy = doDestroy, skipUpdatePath = false)
            // this is a tmp file, so no uri checks needed
            val result = Try(FileUtils.readBytes(tmpPathOpt.get, uriDeepCheck = false)).getOrElse(Array.empty[Byte])
            FileUtils.deleteRecursively(tmpDir, keepRoot = false) // <- delete the tmp context dir
            result
        } finally {
            if (doDestroy) {
                // - handle local path delete if `doDestroy`
                val oldFs = raster.getPathGDAL.asFileSystemPath
                raster.flushAndDestroy()
                FileUtils.tryDeleteLocalContextPath(oldFs, delParentIfContext = true) // <- delete the local fs contents
            }
        }
    }

    /**
     * Write a provided tile to a path, defaults to configured checkpoint
     * dir.
     *   - handles paths (including subdataset paths) as well as hydrated
     *     dataset (regardless of path).
     *
     * @param raster
     *   [[RasterGDAL]]
     * @param doDestroy
     *   Whether to destroy `tile` after write.
     * @return
     *   Return [[UTF8String]]
     */
    def writeRasterAsStringType(
                                   raster: RasterGDAL,
                                   doDestroy: Boolean
                               ): UTF8String = {
        // (1) StringType means we are writing to fuse
        // - override fuse dir would have already been set
        //   on the raster (or not)
        raster.finalizeRaster(toFuse = true)

        // (2) either path or null
        val outPath = raster.getPathOpt match {
            case Some(path) => path
            case _ => null
        }

        // (3) serialize (can handle null)
        UTF8String.fromString(outPath)
    }

}
