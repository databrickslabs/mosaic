package com.databricks.labs.mosaic.core.raster.io

import com.databricks.labs.mosaic.{NO_DRIVER, NO_EXT, NO_PATH_STRING, RASTER_BAND_INDEX_KEY, RASTER_DRIVER_KEY, RASTER_MEM_SIZE_KEY, RASTER_PARENT_PATH_KEY, RASTER_PATH_KEY, RASTER_SUBDATASET_NAME_KEY}
import com.databricks.labs.mosaic.core.raster.api.{FormatLookup, GDAL}
import com.databricks.labs.mosaic.core.raster.gdal.{DatasetGDAL, PathGDAL, RasterBandGDAL, RasterGDAL}
import com.databricks.labs.mosaic.core.raster.io.RasterIO.{identifyDriverNameFromDataset, identifyDriverNameFromRawPath, identifyExtFromDriver}
import com.databricks.labs.mosaic.functions.ExprConfig
import com.databricks.labs.mosaic.utils.{PathUtils, SysUtils}
import org.gdal.gdal.{Dataset, Driver, gdal}
import org.gdal.gdalconst.gdalconstConstants.GA_ReadOnly
import org.gdal.ogr.DataSource

import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.{Vector => JVector}
import scala.util.Try

/**
  * Trait Implemented by [[RasterGDAL]]. Look hardest at top-level functions for
  * usage.
  */
trait RasterIO {

    // ////////////////////////////////////////////////////////////
    // TOP_LEVEL FUNCTIONS
    // ////////////////////////////////////////////////////////////

    /**
      * Use to write out a dataset to fuse and make sure the path is set, e.g.
      * for serialization.
      *   - Impl should also call destroy on the dataset.
      *   - Impl should handle flags.
      *   - Impl should be able to write to fuse dir if specified.
      *
      * @param toFuse
      *   Whether to write to fuse during finalize; if [[RASTER_PATH_KEY]] not already under the specified fuse dir.
      * @return
      *   [[RasterGDAL]] `this` (fluent).
      */
    def finalizeRaster(toFuse: Boolean): RasterGDAL

    /**
      * Call to setup a tile (handle flags): (1) initFlag - if dataset exists,
      * do (2); otherwise do (3). (2) datasetFlag - need to write to fuse and
      * set path. (3) pathFlag - need to load dataset and write to fuse (path
      * then replaced in createInfo).
      *
      * @return
      *   [[RasterGDAL]] `this` (fluent).
      */
    def tryInitAndHydrate(): RasterGDAL

    // ////////////////////////////////////////////////////////////
    // STATE FUNCTIONS
    // ////////////////////////////////////////////////////////////

    /**
      * Destroys the tile object. After this operation the tile object is no
      * longer usable. If the tile is needed again, use the refreshFromPath
      * method.
      * @return
      *   [[RasterGDAL]] `this` (fluent).
      */
    def flushAndDestroy(): RasterGDAL

    /**
      * The driver name as option, first tries from [[DatasetGDAL]] then falls
      * back to `createInfo`.
      */
    def getDriverNameOpt: Option[String]

    /** The dataset option with hydration attempted as needed. */
    def getDatasetOpt(): Option[Dataset]

    /** The [[Dataset]] after hydration attempted or null. */
    def getDatasetOrNull(): Dataset = getDatasetOpt().orNull

    /**
      * This is a simple Getter.
      * @return
      *   returns option for the fuse dir used, None means using latest
      *   configured checkpoint dir.
      */
    def getFuseDirOpt: Option[String]

    /**
      * The path name as option, simple getter. This may be updated along the
      * way.
      */
    def getPathOpt: Option[String]

    /**
      * The parentPath name as option, simple getter. This may be updated along
      * the way.
      */
    def getParentPathOpt: Option[String]

    /** @return current state of GDAL tile dataset object. */
    def isDatasetHydrated: Boolean

    /** @return whether this object is intentionally empty (not the dataset). */
    def isEmptyRasterGDAL: Boolean

    /** @return whether fuse path is / would be in fuse dir. */
    def isRawPathInFuseDir: Boolean

    /**
      * Specify a fuse dir option, e.g. other than configured checkpoint to use.
      *   - pass None to use default.
      *   - This is a config function vs update because it is automatically
      *     handled, unless otherwise configured.
      *
      * @param dirOpt
      *   Option dir to set, may be None which means revert back to configured
      *   checkpoint dir.
      * @return
      *   [[RasterGDAL]] `this` (fluent).
      */
    def setFuseDirOpt(dirOpt: Option[String]): RasterGDAL

    // ///////////////////////////////////////////////////
    // Trait Defined Functions
    // ///////////////////////////////////////////////////

    /**
      * A very common need, to have a tempfile generated with an extension that
      * matches the driver name.
      *
      * @param tryDatasetAndPathsAlso
      *   See [[getDriverName()]], defaults to false.
      * @param exprConfigOpt
      *   Pass option [[ExprConfig]] to use the configured local prefix,
      *   defaults to null.
      * @return
      *   A temp file with the extension - throws a runtime exception if driver
      *   not found.
      */
    def createTmpFileFromDriver(
        exprConfigOpt: Option[ExprConfig],
        tryDatasetAndPathsAlso: Boolean = false
    ): String = {
        val driverName = this.getDriverName(tryDatasetAndPathsAlso = tryDatasetAndPathsAlso)
        val ext = identifyExtFromDriver(driverName)
        PathUtils.createTmpFilePath(ext, exprConfigOpt)
    }

    /**
      * Internal function - variety of checks to try to return a path with an
      * extension.
      *   - tests parent path then path
      *   - default is None, may also be NO_PATH_STRING.<ext>
      *
      * @return
      *   Option string.
      */
    def identifyPseudoPathOpt(): Option[String] =
        Try {
            // !!! avoid cyclic dependencies !!!
            val parentPath = this.getParentPathOpt.getOrElse(NO_PATH_STRING)
            val path = this.getPathOpt.getOrElse(NO_PATH_STRING)
            if (parentPath != NO_PATH_STRING) parentPath
            else if (path != NO_PATH_STRING) path
            else {
                val driverSN = this.getDriverName() // defaults to NO_DRIVER
                val ext = identifyExtFromDriver(driverSN) // defaults to NO_EXT
                if (ext != NO_EXT) s"$NO_PATH_STRING.$ext"
                else null // no viable option
            }
        }.toOption

    /**
      * Convenience method.
      * - If you use this, make sure to delete the driver after use.
      *
      * @return
      *   Option [[Driver]] from hydrated [[Dataset]].
      */
    def tryGetDriverFromDataset(): Option[Driver] =
        Try {
            this.getDatasetOpt().get.GetDriver()
        }.toOption

    /**
      * Rule based test for driver.
      *   - (1) try the dataset's driver (if available)
      *   - (2) try the configured "driver" in createInfo
      *   - (3) fallback to configured "path", then "parentPath" (based on raw
      *     path, e.g. for subdatasets).
      *
      * @param tryDatasetAndPathsAlso
      *   Whether to try (1) and (3) also or just (2), default false.
      * @param uriPartOpt
      *   Option uri part opt for (1) and (3).
      * @return
      *   Driver short name, default is NO_DRIVER.
      */
    def getDriverName(tryDatasetAndPathsAlso: Boolean = false, uriPartOpt: Option[String] = None): String =
        Try {
            if (tryDatasetAndPathsAlso && this.isDatasetHydrated) {
                // (1) try the dataset's driver (if available)
                identifyDriverNameFromDataset(this.getDatasetOrNull())
            } else {
                // driver name from consolidated logic under `datasetGDAL`
                this.getDriverNameOpt match {
                    case Some(driverName) if driverName != NO_DRIVER =>
                        // (2) try the configured "driver" in createInfo
                        driverName
                    case _                                           =>
                        if (tryDatasetAndPathsAlso) {
                            // (3) fallback to configured "path", then "parentPath" (based on raw path, e.g. for subdatasets)
                            var pathDriverName = identifyDriverNameFromRawPath(
                                getPathOpt.getOrElse(NO_PATH_STRING), uriPartOpt
                            )
                            if (pathDriverName == NO_DRIVER) {
                                pathDriverName = identifyDriverNameFromRawPath(
                                    getParentPathOpt.getOrElse(NO_PATH_STRING), uriPartOpt
                                )
                            }
                            pathDriverName
                        } else NO_DRIVER
                }
            }
        }.getOrElse(NO_DRIVER)

}

/**
  * Singleton providing centralized functions for reading / writing tile data
  * to a file system path or as bytes. Also, common support such as identifying
  * a driver or a driver extension.
  */
object RasterIO {

    // ////////////////////////////////////////////////////////
    // DRIVER / EXTENSION
    // ////////////////////////////////////////////////////////

    /**
      * A very common need, to have a tempfile generated with an extension that
      * matches the driver name.
      *
      * @param driverShortName
      *   The driver name to use (e.g. from `getDriverName`).
      * @param exprConfigOpt
      *   Pass option [[ExprConfig]] to use the configured local prefix,
      *   defaults to null.
      * @return
      *   A temp file with the extension - throws a runtime exception if driver
      *   not found.
      */
    def createTmpFileFromDriver(
        driverShortName: String,
        exprConfigOpt: Option[ExprConfig]
    ): String = {
        val ext = identifyExtFromDriver(driverShortName)
        PathUtils.createTmpFilePath(ext, exprConfigOpt)
    }

    /** @return UUID standardized for use in Path or Directory.  */
    def genUUID: String = PathUtils.genUUID

    /** @return filename with UUID standardized for use in Path or Directory (raster_<uuid>.<ext>). */
    def genFilenameUUID(ext: String, uuidOpt: Option[String]): String = PathUtils.genFilenameUUID(ext, uuidOpt)

    /**
      * Identifies the driver of a tile from a file system path.
      *
      * @param aPath
      *   The path to the tile file.
      * @param uriPartOpt
      *   Option uri part.
      * @return
      *   A string representing the driver short name, default [[NO_DRIVER]].
      */
    def identifyDriverNameFromRawPath(aPath: String, uriPartOpt: Option[String]): String =
        Try {
            val readPath = PathUtils.asFileSystemPath(aPath, uriPartOpt)
            val driver = gdal.IdentifyDriverEx(readPath)
            try {
                driver.getShortName
            } finally {
                driver.delete()
            }
        }.getOrElse(NO_DRIVER)

    /**
      * Identifies the driver of a tile from a dataset.
      *
      * @param dataset
      *   Get the driver from dataset.
      * @return
      *   A string representing the driver short name, default [[NO_DRIVER]].
      */
    def identifyDriverNameFromDataset(dataset: Dataset): String =
        Try {
            val driver = dataset.GetDriver()
            try {
                driver.getShortName
            } finally {
                driver.delete()
            }
        }.getOrElse(NO_DRIVER)

    /**
      * @return
      *   Returns driver short name for an extension options. Default
      *   [[NO_DRIVER]]
      */
    def identifyDriverNameFromExtOpt(extOpt: Option[String]): String =
        Try {
            extOpt match {
                case Some(ext) if ext != NO_EXT =>
                    val driver = gdal.IdentifyDriverEx(ext)
                    try {
                        driver.getShortName
                    } finally {
                        driver.delete()
                    }

                case _                          => NO_DRIVER
            }
        }.getOrElse {
            var result = NO_DRIVER

            extOpt match {
                case Some(ext) =>
                    val idx = FormatLookup.formats.values.toList.indexOf(ext)
                    if (idx > -1) result = FormatLookup.formats.keys.toList(idx)
                case _ => ()
            }
            result
        }

    /** @return Returns file extension. default [[NO_EXT]]. */
    def identifyExtFromDriver(driverShortName: String): String =
        Try {
            GDAL.getExtension(driverShortName)
        }.getOrElse(NO_EXT)

    /**
      * @return
      *   Returns file extension (converts to clean path). default [[NO_EXT]].
      */
    def identifyExtFromPath(path: String, uriPartOpt: Option[String]): String =
        Try {
            Paths.get(PathUtils.asFileSystemPath(path, uriPartOpt)).getFileName.toString.split("\\.").last
        }.getOrElse(NO_EXT)

    /** @return Returns file extension. */
    def identifyExtOptFromDataset(dataset: Dataset): Option[String] = {
        if (dataset != null) {
            identifyExtOptFromDriver(dataset.GetDriver(), closeDriver = true)
        } else None
    }

    /** @return Returns file extension. */
    def identifyExtOptFromDriver(driver: Driver, closeDriver: Boolean): Option[String] = {
        val result = Try(GDAL.getExtension(driver.getShortName)).toOption
        if (closeDriver) {
            Try(driver.delete())
        }
        result
    }

    /** @return Returns file extension. */
    def identifyExtOptFromDriver(driverShortName: String): Option[String] = {
        Try(GDAL.getExtension(driverShortName)).toOption
    }

    /**
      * @return
      *   Returns file extension as option (path converted to clean path).
      */
    def identifyExtOptFromPath(path: String, uriPartOpt: Option[String]): Option[String] = {
        PathUtils.getExtOptFromPath(path, uriPartOpt)
    }

    // ////////////////////////////////////////////////////////
    // DATASET
    // ////////////////////////////////////////////////////////

    //scalastyle:off println
    /**
     * Opens a tile from a file system path with a given driver.
     *   - Use the raw path for subdatasets and /vsi* paths.
     *
     * @param pathGDAL
     *   The [[PathGDAL]] to use.
     * @param driverNameOpt
     *   The driver short name to use. If None or NO_DRIVER, GDAL will try to
     *   identify the driver from the file extension.
     * @param exprConfigOpt
     *   Option [[ExprConfig]]
     * @return
     *   A GDAL [[Dataset]] object.
     */
    def rawPathAsDatasetOpt(pathGDAL: PathGDAL, driverNameOpt: Option[String], exprConfigOpt: Option[ExprConfig]): Option[Dataset] =
        Try {

            // various checks to handle
            var driverName = NO_DRIVER
            var hasDriver = driverNameOpt.isDefined && driverNameOpt.get != NO_DRIVER
            if (hasDriver) {
                //println(s"RasterIO - rawPathAsDatasetOpt - driver passed")
                driverName = driverNameOpt.get
            } else {
                //println(s"RasterIO - rawPathAsDatasetOpt - path ext (used in driver)? '${pathGDAL.getExtOpt}', path driver? '${pathGDAL.getPathDriverName}'")
                driverName = pathGDAL.getPathDriverName
                hasDriver = driverName != NO_DRIVER
            }
            val gdalPathOpt = pathGDAL.asGDALPathOpt(Some(driverName))
            val hasGDALPath = gdalPathOpt.isDefined
            val hasSubset = pathGDAL.isSubdataset

            // fallback path (no subdataset with this)
            val fsPath = pathGDAL.asFileSystemPath
            var gdalExSuccess = false
            //println(s"fsPath? '$fsPath' | gdalPath (generated)? '${gdalPathOpt}' | rawGdalUriPart? '${pathGDAL.getRawUriGdalOpt}' driver? '$driverName'")

            var dsOpt = {
                if (hasDriver && hasGDALPath) {
                    // use the provided driver and coerced gdal path
                    try {
                        val gdalPath = gdalPathOpt.get
                        //println(s"RasterIO - rawPathAsDatasetOpt - `gdal.OpenEx` gdalPath? '$gdalPath' (driver? '$driverName')")
                        val drivers = new JVector[String]() // java.util.Vector
                        drivers.add(driverName)
                        val result = gdal.OpenEx(gdalPath, GA_ReadOnly, drivers)
                        if (result != null) gdalExSuccess = true
                        Option(result)
                    } catch {
                        case _: Throwable =>
                            //println(s"RasterIO - rawPathAsDatasetOpt - `gdal.Open` fsPath? '$fsPath'")
                            val result = gdal.Open(fsPath, GA_ReadOnly)
                            Option(result)
                    }
                } else {
                    // just start from the file system path
                    //println(s"RasterIO - rawPathAsDatasetOpt - `gdal.Open` fsPath? '$fsPath'")
                    val result = gdal.Open(fsPath, GA_ReadOnly)
                    Option(result)
                }
            }

            //println(s"dsOpt -> ${dsOpt.toString}")
            if (dsOpt.isDefined && hasSubset && !gdalExSuccess) {
                // try to load the subdataset from the dataset
                // - we got here because the subdataset failed to load,
                //   but the full dataset loaded.
                //println(s"RasterIO - rawPathAsDatasetOpt - subdataset load")
                val dsGDAL = DatasetGDAL()
                try {
                    dsGDAL.updateDataset(dsOpt.get, doUpdateDriver = true)
                    pathGDAL.getSubNameOpt match {
                        case Some(subName) =>
                            val gdalPath = gdalPathOpt.get
                            dsOpt = dsGDAL.getSubdatasetObj(gdalPath, subName, exprConfigOpt).getDatasetOpt // <- subdataset
                        case _ =>
                            dsOpt = None // <- no subdataset
                    }

                } finally {
                    dsGDAL.flushAndDestroy()
                }
            }

            dsOpt
        }.getOrElse(None)
    //scalastyle:on println

    //scalastyle:off println
    /**
      * Opens a tile from a file system path with a given driver.
      *   - Use the raw path for subdatasets and /vsi* paths.
      *   - this just constructs a [[PathGDAL]] and calls the other signature.
      *
      * @param rawPath
      *   The path to the tile file.
      * @param subNameOpt
      *   Option for a subdataset to include.
      * @param driverNameOpt
      *   The driver short name to use. If None or NO_DRIVER, GDAL will try to
      *   identify the driver from the file extension.
      * @param exprConfigOpt
      *   Option [[ExprConfig]]
      * @return
      *   A GDAL [[Dataset]] object.
      */
    def rawPathAsDatasetOpt(rawPath: String, subNameOpt: Option[String], driverNameOpt: Option[String], exprConfigOpt: Option[ExprConfig]): Option[Dataset] = {
        val uriDeepCheck = Try(exprConfigOpt.get.isUriDeepCheck).getOrElse(false)
        val pathGDAL = PathGDAL(path = rawPath, uriDeepCheck)
        subNameOpt match {
            case Some(sub) if sub.nonEmpty =>
                // update subdataset
                pathGDAL.updateSubsetName(sub)
            case _ => ()
        }
        rawPathAsDatasetOpt(pathGDAL, driverNameOpt, exprConfigOpt)
    }
    //scalastyle:on println

    // ////////////////////////////////////////////////////////
    // CLEAN
    // ////////////////////////////////////////////////////////

    /**
      * Flushes the cache and deletes the dataset.
      *   - not a physical deletion, just the JVM object is deleted.
      *   - does not unlink virtual files. For that, use gdal.unlink(path).
      *
      * @param ds
      *   The [[Dataset]] to destroy.
      */
    def flushAndDestroy(ds: Dataset): Unit =
        Try {
            // important to flush prior to delete
            ds.FlushCache()

            val driver = ds.GetDriver()
            try {
                val fileList = ds.GetFileList()

                // Not to be confused with physical deletion
                // - this is just deletes JVM object
                ds.delete()

                // Release any "/vsi*" links.
                fileList.forEach {
                    case f if f.toString.startsWith("/vsi") =>
                        // scalastyle:off println
                        // println(s"... deleting vsi path '$f'")
                        // scalastyle:on println
                        Try(driver.Delete(f.toString))
                    case _                                  => ()
                }
            } finally {
                driver.delete()
            }
        }

    def flushAndDestroy(ds: DataSource): Unit =
        Try {
            ds.FlushCache()
            ds.delete()
        }

    // /////////////////////////////////////////////////////////////////////
    // UNIVERSAL READERS
    // - Single reader for Content
    // - Single reader for Paths
    // /////////////////////////////////////////////////////////////////////

    /**
     * Reads a tile band from a file system path. Reads a subdataset band if
     * the path is to a subdataset.
     * @example
     *   Raster: path = "/path/to/file.tif" Subdataset: path =
     *   "FORMAT:/path/to/file.tif:subdataset"
     * @param bandIndex
     *   The band index to read (1+ indexed).
     * @param createInfo
     *   Map of create info for the tile.
     * @param exprConfigOpt
     *   Option [[ExprConfig]]
     * @return
     *   A [[RasterGDAL]] object.
     */
    def readRasterBand(bandIndex: Int, createInfo: Map[String, String], exprConfigOpt: Option[ExprConfig]): RasterBandGDAL = {
        val tmpRaster = this.readRasterHydratedFromPath(createInfo, exprConfigOpt)
        val result = tmpRaster.getBand(bandIndex)
        tmpRaster.flushAndDestroy()

        result
    }

    /**
      * Reads a tile from a byte array. Expects "driver" in createInfo.
      *   - Populates the tile with a dataset, if able.
      *   - May construct an empty [[RasterGDAL]], test `isEmptyRasterGDAL` and
      *     review error keys in `createInfo`.
      *
      * @param rasterArr
      *   The byte array containing the tile data.
      * @param createInfo
      *   Mosaic creation info of the tile. Note: This is not the same as the
      *   metadata of the tile. This is not the same as GDAL creation options.
      * @param exprConfigOpt
      *   Option [[ExprConfig]]
      * @return
      *   A [[RasterGDAL]] object (test `isEmptyRasterGDAL`).
      */
    def readRasterHydratedFromContent(
        rasterArr: Array[Byte],
        createInfo: Map[String, String],
        exprConfigOpt: Option[ExprConfig]
    ): RasterGDAL = {
        if (
          Option(rasterArr).isEmpty || rasterArr.isEmpty ||
          createInfo.getOrElse(RASTER_DRIVER_KEY, NO_DRIVER) == NO_DRIVER
        ) {
            // (1) handle explicitly empty conditions
            val result = RasterGDAL()
            result.updateError(
              "readRasterUniversalContent - explicitly empty conditions",
              fullMsg = "check tile is non-empty and 'driver' name provided."
            )
            result
        } else {
            // (2) write rasterArr to tmpPath
            val driverName = createInfo(RASTER_DRIVER_KEY)
            val tmpPath = RasterIO.createTmpFileFromDriver(driverName, exprConfigOpt)
            Files.write(Paths.get(tmpPath), rasterArr)

            // (3) Try reading as a tmp file, if that fails, rename as a zipped file
            // - use subdataset if in createInfo
            val subName = createInfo.getOrElse(RASTER_SUBDATASET_NAME_KEY, "")
            val subNameOpt =
                if (subName.nonEmpty) Some(subName)
                else None
            val dataset = RasterIO.rawPathAsDatasetOpt(tmpPath, subNameOpt, Option(driverName), exprConfigOpt).orNull // <- allow null
            if (dataset == null) {
                val zippedPath = s"$tmpPath.zip"
                Files.move(Paths.get(tmpPath), Paths.get(zippedPath), StandardCopyOption.REPLACE_EXISTING)
                val ds1 = RasterIO.rawPathAsDatasetOpt(zippedPath, subNameOpt, Option(driverName), exprConfigOpt).orNull // <- allow null
                if (ds1 == null) {
                    // the way we zip using uuid is not compatible with GDAL
                    // we need to unzip and read the file if it was zipped by us
                    val parentDir = Paths.get(zippedPath).getParent
                    val prompt = SysUtils.runScript(Array("/bin/sh", "-c", s"cd $parentDir && unzip -o $zippedPath -d $parentDir"))
                    // zipped files will have the old uuid name of the tile
                    // we need to get the last extracted file name, but the last extracted file name is not the tile name
                    // we can't list folders due to concurrent writes
                    val ext = GDAL.getExtension(driverName)
                    val lastExtracted = SysUtils.getLastOutputLine(prompt)
                    val unzippedPath = PathUtils.parseUnzippedPathFromExtracted(lastExtracted, ext)
                    val ds2 = RasterIO.rawPathAsDatasetOpt(unzippedPath, subNameOpt, Option(driverName), exprConfigOpt).orNull // <- allow null
                    if (ds2 == null) {
                        // (3d) handle error with bytes
                        // - explicitly empty conditions
                        val result = RasterGDAL()
                        result.updateError(
                          "readRasterUniversalContent - Error reading tile from bytes",
                          fullMsg = prompt._3
                        )
                        result
                    } else {
                        // (3c) second zip was successful
                        RasterGDAL(
                          ds2,
                          exprConfigOpt,
                          createInfo + (
                            RASTER_PATH_KEY -> unzippedPath,
                            RASTER_MEM_SIZE_KEY -> rasterArr.length.toString
                          )
                        )
                    }
                } else {
                    // (3b) first zip was successful
                    RasterGDAL(
                      ds1,
                      exprConfigOpt,
                      createInfo + (
                        RASTER_PATH_KEY -> zippedPath,
                        RASTER_MEM_SIZE_KEY -> rasterArr.length.toString
                      )
                    )
                }
            } else {
                // (3a) dataset was successful
                RasterGDAL(
                  dataset,
                  exprConfigOpt,
                  createInfo + (
                    RASTER_PATH_KEY -> tmpPath,
                    RASTER_MEM_SIZE_KEY -> rasterArr.length.toString
                  )
                )
            }
        }
    }

    /**
      * Reads a tile from a file system path. Reads a subdataset if the path
      * is to a subdataset.
      *   - Populates the tile with a dataset, if able.
      *   - May construct an empty [[RasterGDAL]], test `isEmptyRasterGDAL` and
      *     review error keys in `createInfo`.
      * @example
      *   Raster: path = "/path/to/file.tif" Subdataset: path =
      *   "FORMAT:/path/to/file.tif:subdataset"
      *
      * @param createInfo
      *   Map of create info for the tile.
      * @param exprConfigOpt
      *   Option [[ExprConfig]]
      * @return
      *   A [[RasterGDAL]] object (test `isEmptyRasterGDAL`).
      */
    def readRasterHydratedFromPath(createInfo: Map[String, String], exprConfigOpt: Option[ExprConfig]): RasterGDAL = {

        // (1) initial variables from params
        // - construct a [[PathGDAL]] to assist
        val uriDeepCheck = Try(exprConfigOpt.get.isUriDeepCheck).getOrElse(false)
        val inPathGDAL = PathGDAL(createInfo.getOrElse(RASTER_PATH_KEY, NO_PATH_STRING), uriDeepCheck)
        inPathGDAL.updateSubsetName(createInfo.getOrElse(RASTER_SUBDATASET_NAME_KEY, "")) // <- important to set subset
        val driverNameOpt = createInfo.get(RASTER_DRIVER_KEY)

        if (!inPathGDAL.isPathSetAndExists) {
            // (2) handle explicitly empty conditions
            // - [[NO_PATH_STRING]] hits this condition
            // - also, file not present on file system (via `asFileSystemPath` check),
            //   so don't worry about stripping back a path to "clean" ect... handled by the object
            val result = RasterGDAL()
            result.updateError(
                "readRasterUniversalPath - explicitly empty conditions",
                fullMsg = "check 'path' value provided (does it exist?)."
            )
            result
        } else {
            // (3) attempt to load inPathGDAL to dataset
            this.rawPathAsDatasetOpt(inPathGDAL, driverNameOpt, exprConfigOpt) match {
                case Some(dataset) =>
                    // (4a) dataset was successful
                    // - update the driver name (just in case)
                    RasterGDAL(
                        dataset,
                        exprConfigOpt,
                        createInfo + (
                            RASTER_DRIVER_KEY -> this.identifyDriverNameFromDataset(dataset),
                            RASTER_PARENT_PATH_KEY -> createInfo.getOrElse(RASTER_PARENT_PATH_KEY, NO_PATH_STRING),
                            RASTER_BAND_INDEX_KEY -> createInfo.getOrElse(RASTER_BAND_INDEX_KEY, "-1")
                            )
                    )
                case _ =>
                    // (4b) dataset was unsuccessful
                    // - create empty object
                    val result = RasterGDAL()
                    result.updateError(
                        "readRasterUniversalPath - issue generating dataset from subdataset or filesystem path",
                        fullMsg =
                            s"""
                               |Error reading tile from path: ${inPathGDAL.path}
                               |Error: ${gdal.GetLastErrorMsg()}
                                           """
                    )
                    result
            }
        }
    }

}
