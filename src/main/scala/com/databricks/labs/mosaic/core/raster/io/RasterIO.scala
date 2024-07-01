package com.databricks.labs.mosaic.core.raster.io

import com.databricks.labs.mosaic.{NO_DRIVER, NO_EXT, NO_PATH_STRING, RASTER_DRIVER_KEY, RASTER_MEM_SIZE_KEY, RASTER_PATH_KEY}
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.{DatasetGDAL, PathGDAL, RasterGDAL}
import com.databricks.labs.mosaic.core.raster.io.RasterIO.{
    identifyDriverNameFromDataset,
    identifyDriverNameFromRawPath,
    identifyExtFromDriver
}
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
      *
      * @return
      *   [[RasterGDAL]] `this` (fluent).
      */
    def finalizeRaster(): RasterGDAL

    /**
      * Call to setup a raster (handle flags): (1) initFlag - if dataset exists,
      * do (2); otherwise do (3). (2) datasetFlag - need to write to fuse and
      * set path. (3) pathFlag - need to load dataset and write to fuse (path
      * then replaced in createInfo).
      *
      * @param forceInit
      *   Whether to init no matter if previously have done so, default false.
      * @return
      *   [[RasterGDAL]] `this` (fluent).
      */
    def initAndHydrate(forceInit: Boolean = false): RasterGDAL

    /**
      * This is the main call for getting a hydrated dataset.
      *   - Since it can be null, using an option pattern.
      *   - The goal is to simplify the API surface for the end user, so Impl
      *     will handle flags based on various conventions to identify what is
      *     needed to hydrate.
      *   - NOTE: have to be really careful about cyclic dependencies. Search
      *     "cyclic" here and in [[RasterIO]] for any functions that cannot
      *     themselves call `withDatasetHydratedOpt` as they are invoked from
      *     within handle flags function(s) (same for calling `_datasetHydrated`
      *     in Impl).
      *
      * @return
      *   Option Dataset
      */
    def withDatasetHydratedOpt(): Option[Dataset]

    // ////////////////////////////////////////////////////////////
    // STATE FUNCTIONS
    // ////////////////////////////////////////////////////////////

    /**
      * Destroys the raster object. After this operation the raster object is no
      * longer usable. If the raster is needed again, use the refreshFromPath
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

    /** The dataset option, simple getter. */
    def getDatasetOpt: Option[Dataset]

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

    /** @return current state of GDAL raster dataset object. */
    def isDatasetHydrated: Boolean

    /** @return whether GDAL raster is flagged to be refreshed. */
    def isDatasetRefreshFlag: Boolean

    /** @return whether this object is intentionally empty (not the dataset). */
    def isEmptyRasterGDAL: Boolean

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
      *
      * @return
      *   Option [[Driver]] from hydrated [[Dataset]].
      */
    def tryGetDriverHydrated(): Option[Driver] =
        Try {
            this.withDatasetHydratedOpt().get.GetDriver()
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
      * @return
      *   Driver short name, default is NO_DRIVER.
      */
    def getDriverName(tryDatasetAndPathsAlso: Boolean = false): String =
        Try {
            if (tryDatasetAndPathsAlso && this.isDatasetHydrated) {
                // (1) try the dataset's driver (if available)
                identifyDriverNameFromDataset(this.getDatasetOpt.get)
            } else {
                this.getDriverNameOpt match {
                    case Some(driverName) if driverName != NO_DRIVER =>
                        // (2) try the configured "driver" in createInfo
                        driverName
                    case _                                           =>
                        if (tryDatasetAndPathsAlso) {
                            // (3) fallback to configured "path", then "parentPath" (based on raw path, e.g. for subdatasets)
                            var pathDriverName = identifyDriverNameFromRawPath(getPathOpt.getOrElse(NO_PATH_STRING))
                            if (pathDriverName == NO_DRIVER) {
                                pathDriverName = identifyDriverNameFromRawPath(getParentPathOpt.getOrElse(NO_PATH_STRING))
                            }
                            pathDriverName
                        } else NO_DRIVER
                }
            }
        }.getOrElse(NO_DRIVER)



}

/**
  * Singleton providing centralized functions for reading / writing raster data
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

    /**
      * Identifies the driver of a raster from a file system path.
      *
      * @param aPath
      *   The path to the raster file.
      * @return
      *   A string representing the driver short name, default [[NO_DRIVER]].
      */
    def identifyDriverNameFromRawPath(aPath: String): String =
        Try {
            val readPath = PathUtils.asFileSystemPath(aPath)
            val driver = gdal.IdentifyDriverEx(readPath)
            try {
                driver.getShortName
            } finally {
                driver.delete()
            }
        }.getOrElse(NO_DRIVER)

    /**
      * Identifies the driver of a raster from a dataset.
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
                    val driver = gdal.IdentifyDriverEx(s"$NO_PATH_STRING.$ext")
                    try {
                        driver.getShortName
                    } finally {
                        driver.delete()
                    }
                case _                          => NO_DRIVER
            }
        }.getOrElse(NO_DRIVER)

    /** @return Returns file extension. default [[NO_EXT]]. */
    def identifyExtFromDriver(driverShortName: String): String =
        Try {
            GDAL.getExtension(driverShortName)
        }.getOrElse(NO_EXT)

    /**
      * @return
      *   Returns file extension (converts to clean path). default [[NO_EXT]].
      */
    def identifyExtFromPath(path: String): String =
        Try {
            Paths.get(PathUtils.asFileSystemPath(path)).getFileName.toString.split("\\.").last
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
    def identifyExtOptFromPath(path: String): Option[String] = PathUtils.getExtOptFromPath(path)

    // ////////////////////////////////////////////////////////
    // DATASET
    // ////////////////////////////////////////////////////////

    /**
      * Opens a raster from a file system path with a given driver.
      *   - Use the raw path for subdatasets and /vsi* paths.
      *
      * @param rawPath
      *   The path to the raster file.
      * @param driverNameOpt
      *   The driver short name to use. If None or NO_DRIVER, GDAL will try to
      *   identify the driver from the file extension.
      * @return
      *   A GDAL [[Dataset]] object.
      */
    def rawPathAsDatasetOpt(rawPath: String, driverNameOpt: Option[String]): Option[Dataset] =
        Try {
            // Add [[VSI_ZIP_TOKEN]] (if zip)
            // - handles fuse
            // - this is a safety net to reduce burden on callers
            val path = {
                if (PathUtils.isSubdataset(rawPath)) PathUtils.asSubdatasetGDALPathOpt(rawPath, uriFuseReady = true).get
                else PathUtils.getCleanPath(rawPath, addVsiZipToken = true)
            }

            driverNameOpt match {
                case Some(driverName) if driverName != NO_DRIVER =>
                    // use the provided driver
                    val drivers = new JVector[String]() // java.util.Vector
                    drivers.add(driverName)
                    gdal.OpenEx(path, GA_ReadOnly, drivers)
                case _                                           =>
                    // try just from raw path
                    gdal.Open(path, GA_ReadOnly)
            }
        }.toOption

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
      * Reads a raster from a byte array. Expects "driver" in createInfo.
      *   - Populates the raster with a dataset, if able.
      *   - May construct an empty [[RasterGDAL]], test `isEmptyRasterGDAL` and
      *     review error keys in `createInfo`.
      *
      * @param rasterArr
      *   The byte array containing the raster data.
      * @param createInfo
      *   Mosaic creation info of the raster. Note: This is not the same as the
      *   metadata of the raster. This is not the same as GDAL creation options.
      * @param exprConfigOpt
      *   Option [[ExprConfig]]
      * @return
      *   A [[RasterGDAL]] object (test `isEmptyRasterGDAL`).
      */
    def rasterHydratedFromContent(
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
            result.updateCreateInfoError(
              "readRasterUniversalContent - explicitly empty conditions",
              fullMsg = "check raster is non-empty and 'driver' name provided."
            )
            result
        } else {
            // (2) write rasterArr to tmpPath
            val driverName = createInfo(RASTER_DRIVER_KEY)
            val tmpPath = RasterIO.createTmpFileFromDriver(driverName, exprConfigOpt)
            Files.write(Paths.get(tmpPath), rasterArr)

            // (3) Try reading as a tmp file, if that fails, rename as a zipped file
            val dataset = RasterIO.rawPathAsDatasetOpt(tmpPath, Option(driverName)).orNull // <- allow null
            if (dataset == null) {
                val zippedPath = s"$tmpPath.zip"
                Files.move(Paths.get(tmpPath), Paths.get(zippedPath), StandardCopyOption.REPLACE_EXISTING)
                val readPath = PathUtils.getCleanZipPath(zippedPath, addVsiZipToken = true) // [[VSI_ZIP_TOKEN]] for GDAL
                val ds1 = RasterIO.rawPathAsDatasetOpt(readPath, Option(driverName)).orNull // <- allow null
                if (ds1 == null) {
                    // the way we zip using uuid is not compatible with GDAL
                    // we need to unzip and read the file if it was zipped by us
                    val parentDir = Paths.get(zippedPath).getParent
                    val prompt = SysUtils.runScript(Array("/bin/sh", "-c", s"cd $parentDir && unzip -o $zippedPath -d $parentDir"))
                    // zipped files will have the old uuid name of the raster
                    // we need to get the last extracted file name, but the last extracted file name is not the raster name
                    // we can't list folders due to concurrent writes
                    val ext = GDAL.getExtension(driverName)
                    val lastExtracted = SysUtils.getLastOutputLine(prompt)
                    val unzippedPath = PathUtils.parseUnzippedPathFromExtracted(lastExtracted, ext)
                    val ds2 = RasterIO.rawPathAsDatasetOpt(unzippedPath, Option(driverName)).orNull // <- allow null
                    if (ds2 == null) {
                        // (3d) handle error with bytes
                        // - explicitly empty conditions
                        val result = RasterGDAL()
                        result.updateCreateInfoError(
                          "readRasterUniversalContent - Error reading raster from bytes",
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
                        RASTER_PATH_KEY -> readPath,
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
      * Reads a raster from a file system path. Reads a subdataset if the path
      * is to a subdataset.
      *   - Populates the raster with a dataset, if able.
      *   - May construct an empty [[RasterGDAL]], test `isEmptyRasterGDAL` and
      *     review error keys in `createInfo`.
      * @example
      *   Raster: path = "/path/to/file.tif" Subdataset: path =
      *   "FORMAT:/path/to/file.tif:subdataset"
      *
      * @param createInfo
      *   Map of create info for the raster.
      * @param exprConfigOpt
      *   Option [[ExprConfig]]
      * @return
      *   A [[RasterGDAL]] object (test `isEmptyRasterGDAL`).
      */
    def rasterHydratedFromPath(createInfo: Map[String, String], exprConfigOpt: Option[ExprConfig]): RasterGDAL = {

        // (1) initial variables from params
        // - construct a [[PathGDAL]] to assist
        val inPathGDAL = PathGDAL(createInfo.getOrElse(RASTER_PATH_KEY, NO_PATH_STRING))
        val driverNameOpt = createInfo.get(RASTER_DRIVER_KEY)

        if (!inPathGDAL.isPathSetAndExists) {
            // (2) handle explicitly empty conditions
            // - [[NO_PATH_STRING]] hits this condition
            // - also, file not present on file system (via `asFileSystemPath` check),
            //   so don't worry about stripping back a path to "clean" ect... handled by the object
            val result = RasterGDAL()
            result.updateCreateInfoError(
              "readRasterUniversalPath - explicitly empty conditions",
              fullMsg = "check 'path' value provided (does it exist?)."
            )
            result
        } else {
            // (3) Prep for a subdataset path or a filesystem path
            // - both of these handle fuse (e.g. if URISchema part of raw path)
            val readPathOpt = {
                if (inPathGDAL.isSubdatasetPath) inPathGDAL.asSubdatasetGDALFuseOpt
                else inPathGDAL.asFileSystemPathOpt
            }
            // (4) load readPath to dataset
            readPathOpt match {
                case Some(readPath) => this.rawPathAsDatasetOpt(readPath, driverNameOpt) match {
                        case Some(dataset) =>
                            // (4a) dataset was successful
                            RasterGDAL(
                              dataset,
                              exprConfigOpt,
                              createInfo
                            )
                        case _             =>
                            // (4b) dataset was unsuccessful
                            // - create empty object
                            val result = RasterGDAL()
                            result.updateCreateInfoError(
                              "readRasterUniversalPath - issue generating dataset from subdataset or filesystem path",
                              fullMsg = s"""
                                             |Error reading raster from path: $readPath
                                             |Error: ${gdal.GetLastErrorMsg()}
                                           """
                            )
                            result
                    }
                case _              =>
                    // (4c) the initial option unsuccessful
                    val result = RasterGDAL()
                    result.updateCreateInfoError(
                      "readRasterUniversalPath - issue generating subdataset or filesystem path",
                      fullMsg = s"check initial path '${inPathGDAL.path}' ."
                    )
                    result
            }
        }
    }

    // ////////////////////////////////////////////////////////////
    // ??? ARE THESE NEEDED ???
    // ////////////////////////////////////////////////////////////
//
//    /**
//     * This is a simple Getter.
//     * @return
//     *   returns option for the fuse dir used, None means using latest
//     *   configured checkpoint dir.
//     */
//    def getFusePathOpt: Option[String]
//
//    /** @return whether fuse path has same extension, default is false. */
//    def isPathExtMatchFuse: Boolean
//
//    /** @return whether fuse is available for loading as dataset. */
//    def isFusePathSetAndExists: Boolean
//
//    /**
//     * @return
//     *   whether fuse path is / would be in fuse dir (following RasterIO
//     *   conventions).
//     */
//    def isFusePathInFuseDir: Boolean
//
//    /**
//     * @return
//     *   whether the path is the same as the fuse path (false if either are
//     *   None).
//     */
//    def isCreateInfoPathSameAsFuse: Boolean
//
//    /**
//     * Fuse path which will be used in persisting the raster
//     *   - This does not generate a new path, just conditionally might
//     *     invalidate an existing path (make None).
//     *   - Use Impl `_handleFlags` or higher methods `withDatasetHydrated` or
//     *     `finalizeRaster` to actually perform the writes.
//     *
//     * @param forceNone
//     *   For various externals that require a new fuse path (based on latest
//     *   fuse `config` settings). Will invalidate existing path.
//     *   - This does not generate a new path.
//     *
//     * @return
//     *   [[RasterGDAL]] `this` (fluent).
//     */
//    def configFusePathOpt(forceNone: Boolean): RasterGDAL
//
//    /**
//     * Set new path.
//     *   - invalidates existing paths (local and fuse) or dataset.
//     *
//     * @param rawPath
//     *   path to set.
//     * @param fuseDirOverrideOpt
//     *   If option provide, set / use the specified fuse directory.
//     * @return
//     *   [[RasterGDAL]] `this` (fluent).
//     */
//    def configNewRawPath(
//                            rawPath: String,
//                            fuseDirOverrideOpt: Option[String] = None
//                        ): RasterGDAL
//
//    /** @inheritdoc */
//    override def isPathExtMatchFuse: Boolean =
//        Try {
//            datasetGDAL.pathGDAL.getExtOpt.get == fuseGDAL.getExtOpt.get
//        }.getOrElse(false)
//
//    /** @inheritdoc */
//    override def isFusePathSetAndExists: Boolean = fuseGDAL.isPathSetAndExists
//
//    /** @inheritdoc */
//    override def isFusePathInFuseDir: Boolean =
//        Try {
//            // !!! avoid cyclic dependencies !!!
//            // - wrapped to handle false conditions
//            this.fuseDirOpt match {
//                case Some(dir) => this.fuseGDAL.path.startsWith(dir)
//                case _         => this.fuseGDAL.path.startsWith(GDAL.getCheckpointDir)
//            }
//        }.getOrElse(false)
//
//    /** @inheritdoc */
//    override def isCreateInfoPathSameAsFuse: Boolean =
//        Try {
//            // !!! avoid cyclic dependencies !!!
//            this.getRawPath == fuseGDAL.path
//        }.getOrElse(false)
//
//    /** fuse path option to None; returns `this` (fluent). */
//    def resetFusePathOpt(): RasterGDAL = {
//        fuseGDAL.resetPath
//        this
//    }

}
