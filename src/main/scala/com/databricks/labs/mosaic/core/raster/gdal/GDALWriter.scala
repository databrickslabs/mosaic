package com.databricks.labs.mosaic.core.raster.gdal

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.io.RasterIO
import com.databricks.labs.mosaic.functions.ExprConfig
import com.databricks.labs.mosaic.utils.{FileUtils, SysUtils}
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
     *   The type of raster to write.
     *   - if string write to checkpoint
     *   - otherwise, write to bytes
     * @param doDestroy
     *   Whether to destroy the internal object after serializing.
     * @param exprConfigOpt
     *   Option [[ExprConfig]]
     * @param overrideDirOpt
     *   Option String, default is None.
     *   - if provided, where to write the raster.
     *   - only used with rasterDT of [[StringType]]
     * @return
     *   Returns the paths of the written rasters.
     */
    def writeRasters(
                        rasters: Seq[RasterGDAL],
                        rasterDT: DataType,
                        doDestroy: Boolean,
                        exprConfigOpt: Option[ExprConfig],
                        overrideDirOpt: Option[String]
                    ): Seq[Any]


    // ///////////////////////////////////////////////////////////////
    // Writers for [[BinaryType]] and [[StringType]]
    // ///////////////////////////////////////////////////////////////

    /**
     * Writes a raster to a byte array.
     *
     * @param raster
     *   The [[RasterGDAL]] object that will be used in the write.
     * @param doDestroy
     *   A boolean indicating if the raster object should be destroyed after
     *   writing.
     *   - file paths handled separately.
     * @param exprConfigOpt
     *   Option [[ExprConfig]]
     * @return
     *   A byte array containing the raster data.
     */
    def writeRasterAsBinaryType(
                                   raster: RasterGDAL,
                                   doDestroy: Boolean,
                                   exprConfigOpt: Option[ExprConfig]
                               ): Array[Byte] =
        Try {
            val datasetGDAL = raster.getDatasetGDAL
            val pathGDAL = raster.getPathGDAL

            // (1) subdataset or "normal" filesystem path
            val tmpPath: String = {
                if (pathGDAL.isSubdatasetPath) {
                    raster.withDatasetHydratedOpt() match {
                        case Some(dataset) =>
                            val tmpPath1 = RasterIO.createTmpFileFromDriver(
                                datasetGDAL.driverNameOpt.get, // <- driver should be valid
                                exprConfigOpt
                            )
                            datasetGDAL.datasetCopyToPath(tmpPath1, doDestroy = false) // <- destroy 1x at end
                            tmpPath1
                        case _ =>
                            pathGDAL.asFileSystemPath // <- get a filesystem path
                    }
                } else {
                    pathGDAL.asFileSystemPath // <- get a filesystem path
                }
            }

            // (2) handle directory
            //  - must zip
            val readPath: String = {
                val readJavaPath = Paths.get(tmpPath)
                if (Files.isDirectory(readJavaPath)) {
                    val parentDir = readJavaPath.getParent.toString
                    val fileName = readJavaPath.getFileName.toString
                    val prompt = SysUtils.runScript(Array("/bin/sh", "-c", s"cd $parentDir && zip -r0 $fileName.zip $fileName"))
                    if (prompt._3.nonEmpty) {
                        throw new Exception(
                            s"Error zipping file: ${prompt._3}. Please verify that zip is installed. Run 'apt install zip'."
                        )
                    }
                    s"$tmpPath.zip"
                } else {
                    tmpPath
                }
            }
            val byteArray = FileUtils.readBytes(readPath)

            if (doDestroy) raster.flushAndDestroy()
            byteArray
        }.getOrElse(Array.empty[Byte])

    /**
     * Write a provided raster to a path, defaults to configured checkpoint
     * dir.
     *   - handles paths (including subdataset paths) as well as hydrated
     *     dataset (regardless of path).
     *
     * @param raster
     *   [[RasterGDAL]]
     * @param doDestroy
     *   Whether to destroy `raster` after write.
     * @param overrideDirOpt
     *   Option to override the dir to write to, defaults to checkpoint.
     * @return
     *   Return [[UTF8String]]
     */
    def writeRasterAsStringType(
                                   raster: RasterGDAL,
                                   doDestroy: Boolean,
                                   overrideDirOpt: Option[String]
                               ): UTF8String = {

        val datasetGDAL = raster.getDatasetGDAL
        val pathGDAL = raster.getPathGDAL

        val outPath = {
            if (pathGDAL.isSubdatasetPath) {
                // (1) handle subdataset
                raster.withDatasetHydratedOpt() match {
                    case Some(dataset) =>
                        val uuid = UUID.randomUUID().toString
                        val ext = GDAL.getExtension(datasetGDAL.driverNameOpt.get) // <- driver should be valid
                        val writePath = overrideDirOpt match {
                            case Some(d) => s"$d/$uuid.$ext"
                            case _ => s"${GDAL.getCheckpointDir}/$uuid.$ext"
                        }
                        // copy dataset to specified path
                        // - destroy 1x at end
                        if (datasetGDAL.datasetCopyToPath(writePath, doDestroy = false)) {
                            writePath
                        } else {
                            raster.updateCreateInfoError(s"writeRasterAsStringType - unable to write to subdataset path '$writePath'")
                            null
                        }
                    case _ =>
                        raster.updateCreateInfoError(s"writeRasterAsStringType - unable to write to subdataset path (dataset couldn't be hydrated)")
                        null
                }
            } else {
                // (2) handle normal path-based write
                val writeDir = overrideDirOpt match {
                    case Some(d) => d
                    case _       => GDAL.getCheckpointDir
                }
                pathGDAL.rawPathWildcardCopyToDir(writeDir, doDestroy) match {
                    case Some(path) => path
                    case _          =>
                        raster.updateCreateInfoError(s"writeRasterString - unable to write to dir '$writeDir'")
                        null
                }
            }
        }

        UTF8String.fromString(outPath) // <- can handle null
    }

}
