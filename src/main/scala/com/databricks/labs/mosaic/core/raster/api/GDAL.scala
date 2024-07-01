package com.databricks.labs.mosaic.core.raster.api

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.gdal.RasterGDAL.DIR_TIME_FORMATTER
import com.databricks.labs.mosaic.{NO_PATH_STRING, RASTER_DRIVER_KEY, RASTER_LAST_ERR_KEY, RASTER_PARENT_PATH_KEY, RASTER_PATH_KEY, RASTER_SUBDATASET_NAME_KEY}
import com.databricks.labs.mosaic.core.raster.gdal.{GDALReader, GDALWriter, RasterBandGDAL, RasterGDAL}
import com.databricks.labs.mosaic.core.raster.io.RasterIO
import com.databricks.labs.mosaic.core.raster.operator.clip.RasterClipByVector
import com.databricks.labs.mosaic.core.raster.operator.transform.RasterTransform
import com.databricks.labs.mosaic.functions.ExprConfig
import com.databricks.labs.mosaic.gdal.MosaicGDAL
import com.databricks.labs.mosaic.gdal.MosaicGDAL.configureGDAL
import com.databricks.labs.mosaic.utils.{FileUtils, PathUtils, SysUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants._
import org.gdal.osr

import java.nio.file.{Files, Paths}
import java.time.LocalDateTime
import java.util.UUID
import scala.sys.process._
import scala.util.Try

/** GDAL Raster API. */
object GDAL extends RasterTransform
    with GDALReader
    with GDALWriter {

    /** @return Returns the name of the raster API. */
    val name: String = "GDAL"

    // ///////////////////////////////////////////////////////////////
    // TOP-TIER (ENTRY) FUNCTIONS
    // ///////////////////////////////////////////////////////////////

    /**
      * Enables GDAL on the worker nodes. GDAL requires drivers to be registered
      * on the worker nodes. This method registers all the drivers on the worker
      * nodes.
      *
      * @param exprConfig
      *   The [[ExprConfig]] for the op.
      */
    def enable(exprConfig: ExprConfig): Unit = {
        configureGDAL(exprConfig)
        gdal.UseExceptions()
        gdal.AllRegister()
    }

    /**
      * Enables GDAL on the worker nodes. GDAL requires drivers to be registered
      * on the worker nodes. This method registers all the drivers on the worker
      * nodes.
      *
      * @param spark
      *   Spark session from which to populate the [[ExprConfig]].
      */
    def enable(spark: SparkSession): Unit = {
        val exprConfig = ExprConfig(spark)
        enable(exprConfig)
    }

    /** @inheritdoc */
    override def readRasterExpr(
                      inputRaster: Any,
                      createInfo: Map[String, String],
                      inputDT: DataType,
                      exprConfigOpt: Option[ExprConfig]
                  ): RasterGDAL = {
        if (inputRaster == null) {
            RasterGDAL() // <- (1) empty raster
        } else {
            inputDT match {
                case _: StringType =>
                    // ::: STRING TYPE :::
                    try {
                        RasterIO.rasterHydratedFromPath(
                            createInfo,
                            exprConfigOpt
                        ) // <- (2a) from path
                    } catch {
                        case _: Throwable =>
                            RasterIO.rasterHydratedFromContent(
                                inputRaster.asInstanceOf[Array[Byte]],
                                createInfo,
                                exprConfigOpt
                            ) // <- (2b) from bytes
                    }
                case _: BinaryType =>
                    // ::: BINARY TYPE :::
                    try {
                        RasterIO.rasterHydratedFromContent(
                            inputRaster.asInstanceOf[Array[Byte]],
                            createInfo,
                            exprConfigOpt
                        ) // <- (3a) from bytes
                    } catch {
                        case _: Throwable =>
                            RasterIO.rasterHydratedFromPath(
                                createInfo,
                                exprConfigOpt
                            ) // <- (3b) from path
                    }
                case _             => throw new IllegalArgumentException(s"Unsupported data type: $inputDT")
            }
        }
    }

    /** @inheritdoc */
    override def writeRasters(
                        rasters: Seq[RasterGDAL],
                        rasterDT: DataType,
                        doDestroy: Boolean,
                        exprConfigOpt: Option[ExprConfig],
                        overrideDirOpt: Option[String]
    ): Seq[Any] = {
        rasters.map(raster =>
            if (raster != null) {
                rasterDT match {
                    case StringType => writeRasterAsStringType(raster, doDestroy, overrideDirOpt)
                    case BinaryType => writeRasterAsBinaryType(raster, doDestroy, exprConfigOpt)
                }
            } else {
                null
            }
        )
    }


    // ///////////////////////////////////////////////////////////////
    // CONVENIENCE CREATE FUNCTIONS
    // ///////////////////////////////////////////////////////////////

    /**
     * Reads a raster from the given path. It extracts the specified band from
     * the raster. If zip, use band(path, bandIndex, vsizip = true)
     *
     * @param path
     *   The path to the raster. This path has to be a path to a single raster.
     *   Rasters with subdatasets are supported.
     * @param bandIndex
     *   The index of the band to read from the raster.
     * @param parentPath
     *   Parent path can help with detecting driver.
     * @param exprConfigOpt
     *   Option [ExprConfig]
     * @return
     *   Returns a [[RasterBandGDAL]] object.
     */
    def band(path: String, bandIndex: Int, parentPath: String, exprConfigOpt: Option[ExprConfig]): RasterBandGDAL = {
        // TODO - Should this be an Opt?
        val tmpRaster = RasterIO.rasterHydratedFromPath(
            Map(
                RASTER_PATH_KEY -> path,
                RASTER_PARENT_PATH_KEY -> parentPath
            ),
            exprConfigOpt
        )
        val result = tmpRaster.getBand(bandIndex)
        tmpRaster.flushAndDestroy()

        result
    }

    /**
     * Reads a raster from the given byte array. If the byte array is a zip
     * file, it will read the raster from the zip file.
     *
     * @param content
     *   The byte array to read the raster from.
     * @param parentPath
     *   Parent path can help with detecting driver.
     * @param driverShortName
     *   Driver to use in reading.
     * @param exprConfigOpt
     *   Option [[ExprConfig]]
     * @return
     *   Returns a [[RasterGDAL]] object.
     */
    def raster(
                  content: Array[Byte],
                  parentPath: String,
                  driverShortName: String,
                  exprConfigOpt: Option[ExprConfig]
              ): RasterGDAL = {

        RasterIO.rasterHydratedFromContent(
            content,
            createInfo = Map(
                RASTER_PARENT_PATH_KEY -> parentPath,
                RASTER_DRIVER_KEY -> driverShortName
            ),
            exprConfigOpt
        )
    }

    /**
     * Reads a raster from the given path. Assume not zipped file. If zipped,
     * use raster(path, vsizip = true)
     *
     * @param path
     *   The path to the raster. This path has to be a path to a single raster.
     *   Rasters with subdatasets are supported.
     * @param parentPath
     *   Parent path can help with detecting driver.
     * @param exprConfigOpt
     *   Option [[ExprConfig]]
     * @return
     *   Returns a [[RasterGDAL]] object.
     */
    def raster(path: String, parentPath: String, exprConfigOpt: Option[ExprConfig]): RasterGDAL = {
        RasterIO.rasterHydratedFromPath(
            Map(
                RASTER_PATH_KEY -> path,
                RASTER_PARENT_PATH_KEY -> parentPath
            ),
            exprConfigOpt
        )
    }

    // ///////////////////////////////////////////////////////////////
    // ADDITIONAL FUNCTIONS
    // ///////////////////////////////////////////////////////////////

    /**
      * Cleanup the working directory using configured age in minutes, 0 for
      * now, -1 for never.
      *   - can be manually invoked, e.g. from a notebook after a table has been
      *     generated and it is safe to remove the interim files.
      *   - configured manual mode causes deletes to be skipped, leaving you to
      *     option to occasionally "manually" invoke this function to clean up
      *     the configured mosaic dir, e.g. `/tmp/mosaic_tmp`.
      *   - doesn't do anything if this is a fuse location (/dbfs, /Volumes,
      *     /Workspace)
      *
      * @param ageMinutes
      *   file age (relative to now) to trigger deletion.
      * @param dir
      *   directory [[String]] to delete (managed works at the configured local
      *   raster dir.
      * @param keepRoot
      *   do you want to ensure the directory is created?
      */
    def cleanUpManualDir(
                            ageMinutes: Int,
                            dir: String,
                            keepRoot: Boolean = false,
                            allowFuseDelete: Boolean = false
                        ): Option[String] = {
        try {
            val dirPath = Paths.get(dir)
            if (
              (allowFuseDelete || !PathUtils.isFusePathOrDir(dir)) &&
              Files.exists(dirPath) && Files.isDirectory(dirPath)
            ) {
                ageMinutes match {
                    case now if now == 0 =>
                        // run cmd and capture the output
                        val err = new StringBuilder()
                        val procLogger = ProcessLogger(_ => (), err append _)
                        if (keepRoot) s"rm -rf $dir/*" ! procLogger
                        else s"rm -rf $dir" ! procLogger
                        if (err.length() > 0) Some(err.toString())
                        else None
                    case age if age > 0  =>
                        FileUtils.deleteRecursivelyOlderThan(dirPath, age, keepRoot = keepRoot)
                        None
                    case _               => None
                }
            } else None
        } catch {
            case t: Throwable => Some(t.toString)
        } finally {
            if (keepRoot) Try(s"mkdir -p $dir".!)
        }
    }

    /** @return Returns checkpoint dir (assumes `enable` called) */
    def getCheckpointDir: String = MosaicGDAL.getCheckpointDir

    /**
      * Returns the extension of the given driver.
      *
      * @param driverShortName
      *   The short name of the driver. For example, GTiff.
      * @return
      *   Returns the extension of the driver. For example, tif.
      */
    def getExtension(driverShortName: String): String = {
        val driver = gdal.GetDriverByName(driverShortName)
        try {
            val result = driver.GetMetadataItem("DMD_EXTENSION")
            if (result == null) FormatLookup.formats(driverShortName) else result
        } finally {
            driver.delete()
        }
    }

    /**
      * Returns the no data value for the given GDAL data type. For non-numeric
      * data types, it returns 0.0. For numeric data types, it returns the
      * minimum value of the data type. For unsigned data types, it returns the
      * maximum value of the data type.
      *
      * @param gdalType
      *   The GDAL data type.
      * @return
      *   Returns the no data value for the given GDAL data type.
      */
    def getNoDataConstant(gdalType: Int): Double = {
        gdalType match {
            case GDT_Unknown => 0.0
            case GDT_Byte    => 0.0
            // Unsigned Int16 is Char in scala
            // https://www.tutorialspoint.com/scala/scala_data_types.htm
            case GDT_UInt16  => Char.MaxValue.toDouble
            case GDT_Int16   => Short.MinValue.toDouble
            case GDT_UInt32  => 2 * Int.MaxValue.toDouble
            case GDT_Int32   => Int.MinValue.toDouble
            case GDT_Float32 => Float.MinValue.toDouble
            case GDT_Float64 => Double.MinValue
            case _           => 0.0
        }
    }

    /** @return Returns whether using checkpoint (assumes `enable` called) */
    def isUseCheckpoint: Boolean = MosaicGDAL.isUseCheckpoint


    // ///////////////////////////////////////////////////////////////
    // ???  CAN WE CONSOLIDATE THESE FUNCTIONS ???
    // ///////////////////////////////////////////////////////////////

    /** @return new fuse path string, defaults to under checkpoint dir (doesn't actually create the file). */
    def makeNewFusePath(ext: String, overrideFuseDirOpt: Option[String]): String = {
            // (1) uuid used in dir and filename
            val uuid = UUID.randomUUID().toString

            // (2) new dir under fuse dir (<timePrefix>_<uuid>.<ext>)
            val rootDir = overrideFuseDirOpt.getOrElse(GDAL.getCheckpointDir)
            val timePrefix = LocalDateTime.now().format(DIR_TIME_FORMATTER)
            val newDir = s"${timePrefix}_${ext}_${uuid}"
            val fuseDir = s"$rootDir/$newDir"

            // (3) return the new fuse path name
            s"$fuseDir/$uuid.$ext"
    }


    // TODO - 0.4.3 - is this needed?






    // TODO - 0.4.3 - is this needed?



    //    /**
//     * Try to write to path.
//     *   - this does not call `withDatasetHydrated` to avoid cyclic
//     *     dependencies.
//     *   - this is not "smart", just either writes to existing fuseGDAL if it defined or tries to generate a fresh one.
//     * @param path
//     *   Path to try to write to.
//     * @return
//     *   boolean for success / failure.
//     */
//    private def _tryWriteDatasetToPath(path: String): Boolean =
//        Try {
//            // !!! avoid cyclic dependencies !!!
//            val dataset = datasetOpt.get
//            val driver = dataset.GetDriver()
//            try {
//                val tmpDs = driver.CreateCopy(path, dataset, 1)
//                RasterIO.flushAndDestroy(tmpDs)
//                true
//            } finally {
//                driver.delete()
//            }
//        }.getOrElse(false)
//
//    /**
//     * Try to write to internally managed fuse path.
//     *   - this does not call `withDatasetHydrated` to avoid cyclic
//     *     dependencies.
//     *   - this is not "smart", just either writes to existing fuseGDAL if it defined or tries to generate a fresh one.
//     * @return
//     *   boolean for success / failure.
//     */
//    private def _tryWriteDatasetToFusePath(): Boolean =
//        Try {
//            // !!! avoid cyclic dependencies !!!
//            // attempt to get / config fuse path
//            // - this should be a file (<uuid>.<ext>) within its own dir under fuseDirOpt
//            fusePathOpt match {
//                case Some(path) => () // all good
//                case _  => this._configNewFusePathOpt
//            }
//            _tryWriteDatasetToPath(fusePathOpt.get)
//        }.getOrElse(false)
//
//    /**
//     * Try to write to a PathUtils generated tmp path.
//     *   - this does not call `withDatasetHydrated` to avoid cyclic
//     *     dependencies.
//     *    - this is not "smart", just either writes to fuseGDAL if it isDefined or generates a fresh one.
//     * @return
//     *   Option for path string depending on success / failure.
//     */
//    private def _tryWriteDatasetToTmpPath(): Option[String] =
//        Try {
//            // !!! avoid cyclic dependencies !!!
//            val dataset = datasetOpt.get
//            val driver = dataset.GetDriver()
//            try {
//                val path = this.createTmpFileFromDriver(exprConfigOpt)
//                val tmpDs = driver.CreateCopy(path, dataset, 1)
//                RasterIO.flushAndDestroy(tmpDs)
//                path
//            } finally {
//                driver.delete()
//            }
//        }.toOption

}
