package com.databricks.labs.mosaic.core.raster.api

import com.databricks.labs.mosaic.{RASTER_DRIVER_KEY, RASTER_PARENT_PATH_KEY, RASTER_PATH_KEY}
import com.databricks.labs.mosaic.core.raster.gdal.{GDALReader, GDALWriter, RasterBandGDAL, RasterGDAL}
import com.databricks.labs.mosaic.core.raster.io.RasterIO
import com.databricks.labs.mosaic.core.raster.operator.transform.RasterTransform
import com.databricks.labs.mosaic.functions.ExprConfig
import com.databricks.labs.mosaic.gdal.MosaicGDAL
import com.databricks.labs.mosaic.gdal.MosaicGDAL.configureGDAL
import com.databricks.labs.mosaic.utils.{FileUtils, PathUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants._

import java.nio.file.{Files, Paths}
import scala.sys.process._
import scala.util.Try

/** GDAL Raster API. */
object GDAL extends RasterTransform
    with GDALReader
    with GDALWriter {

    /** @return Returns the name of the tile API. */
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
        configureGDAL(Option(exprConfig))
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

    //scalastyle:off println
    /** @inheritdoc */
    override def readRasterExpr(
                      inputRaster: Any,
                      createInfo: Map[String, String],
                      inputDT: DataType,
                      exprConfigOpt: Option[ExprConfig]
                  ): RasterGDAL = {
        if (inputRaster == null) {
            RasterGDAL() // <- (1) empty tile
        } else {
            inputDT match {
                case _: StringType =>
                    // ::: STRING TYPE :::
                    try {
                        //println("GDAL - readRasterExpr - attempting deserialize from path...")
                        RasterIO.readRasterHydratedFromPath(
                            createInfo,
                            exprConfigOpt
                        ) // <- (2a) from path
                    } catch {
                        case _: Throwable =>
                            //println(s"GDAL - readRasterExpr - exception with path, try as bytes...")
                            RasterIO.readRasterHydratedFromContent(
                                inputRaster.asInstanceOf[Array[Byte]],
                                createInfo,
                                exprConfigOpt
                            ) // <- (2b) from bytes
                    }
                case _: BinaryType =>
                    // ::: BINARY TYPE :::
                    try {
                        //println("GDAL - readRasterExpr - attempting deserialize from bytes...")
                        RasterIO.readRasterHydratedFromContent(
                            inputRaster.asInstanceOf[Array[Byte]],
                            createInfo,
                            exprConfigOpt
                        ) // <- (3a) from bytes
                    } catch {
                        case _: Throwable =>
                            //println(s"GDAL - readRasterExpr - exception with bytes, try as path...")
                            RasterIO.readRasterHydratedFromPath(
                                createInfo,
                                exprConfigOpt
                            ) // <- (3b) from path
                    }
                case _             => throw new IllegalArgumentException(s"Unsupported data type: $inputDT")
            }
        }
    }
    //scalastyle:on println

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
     * Reads a tile from the given path. It extracts the specified band from
     * the tile. If zip, use band(path, bandIndex, vsizip = true)
     *
     * @param path
     *   The path to the tile. This path has to be a path to a single tile.
     *   Rasters with subdatasets are supported.
     * @param bandIndex
     *   The index of the band to read from the tile.
     * @param parentPath
     *   Parent path can help with detecting driver.
     * @param exprConfigOpt
     *   Option [ExprConfig]
     * @return
     *   Returns a [[RasterBandGDAL]] object.
     */
    def band(path: String, bandIndex: Int, parentPath: String, exprConfigOpt: Option[ExprConfig]): RasterBandGDAL = {
        val tmpRaster = RasterIO.readRasterHydratedFromPath(
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
     * Reads a tile from the given byte array. If the byte array is a zip
     * file, it will read the tile from the zip file.
     *
     * @param content
     *   The byte array to read the tile from.
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

        RasterIO.readRasterHydratedFromContent(
            content,
            createInfo = Map(
                RASTER_PARENT_PATH_KEY -> parentPath,
                RASTER_DRIVER_KEY -> driverShortName
            ),
            exprConfigOpt
        )
    }

    /**
     * Reads a tile from the given path. Assume not zipped file. If zipped,
     * use tile(path, vsizip = true)
     *
     * @param path
     *   The path to the tile. This path has to be a path to a single tile.
     *   Rasters with subdatasets are supported.
     * @param parentPath
     *   Parent path can help with detecting driver.
     * @param exprConfigOpt
     *   Option [[ExprConfig]]
     * @return
     *   Returns a [[RasterGDAL]] object.
     */
    def raster(path: String, parentPath: String, exprConfigOpt: Option[ExprConfig]): RasterGDAL = {
        RasterIO.readRasterHydratedFromPath(
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
     * Cleans up the tile driver and references.
     *   - This will not clean up a file stored in a Databricks location,
     *     meaning DBFS, Volumes, or Workspace paths are skipped. Unlinks the
     *     tile file. After this operation the tile object is no longer
     *     usable. To be used as last step in expression after writing to
     *     bytes.
     *  - 0.4.2 - don't delete any fuse locations.
     */
    @deprecated("0.4.3 recommend to let CleanUpManager handle")
    def safeCleanUpRasterPath(aPath: String, raster: RasterGDAL, allowThisPathDelete: Boolean, uriDeepCheck: Boolean): Unit = {
        // (1) uri part
        val uriGdalOpt = PathUtils.parseGdalUriOpt(aPath, uriDeepCheck)

        // (1) get file system paths
        val aPathFS = PathUtils.asFileSystemPath(aPath, uriGdalOpt)
        val pPathFS = PathUtils.asFileSystemPath(raster.getRawParentPath, uriGdalOpt)
        val pathFS = PathUtils.asFileSystemPath(raster.getRawPath, uriGdalOpt)

        // (2) checks:
        // - (a) not a fuse location
        // - (b) not the tile parent path
        // - (c) not the tile path (unless allowed)
        if (
            !PathUtils.isFusePathOrDir(aPathFS, uriGdalOpt) && pPathFS != aPathFS &&
                (pathFS != aPathFS || allowThisPathDelete)
        ) {
            raster.getDriverNameOpt match {
                case Some(driverName) =>
                    // (3) use driver to delete the GDAL path
                    // - (a) strips subdataset name
                    // - (b) adds [[VSI_ZIP_TOKEN]]
                    val driver = gdal.GetDriverByName(driverName)
                    try {
                        val gPath = PathUtils.getCleanPath(aPath, addVsiZipToken = true, uriGdalOpt)
                        Try(driver.Delete(gPath))
                    } finally {
                        driver.delete()
                    }
                case _ => ()
            }
            // (4) more complete path cleanup
            PathUtils.cleanUpPath(aPath, uriGdalOpt)
        }
    }

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
      *   tile dir.
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
            // uriGdalOpt is None since this is file system op.
            if (
              (allowFuseDelete || !PathUtils.isFusePathOrDir(dir, uriGdalOpt = None)) &&
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

}
