package com.databricks.labs.mosaic.core.raster.io

//import com.databricks.labs.mosaic.NO_DRIVER
//
//import scala.util.Try
//import com.databricks.labs.mosaic.{NO_DRIVER, RASTER_DRIVER_KEY, RASTER_LAST_ERR_KEY, RASTER_MEM_SIZE_KEY, RASTER_PATH_KEY}
//import com.databricks.labs.mosaic.core.raster.api.GDAL
//import com.databricks.labs.mosaic.core.raster.api.GDAL.getCheckpointDir
//import com.databricks.labs.mosaic.core.raster.gdal.{RasterBandGDAL, RasterGDAL}
//import com.databricks.labs.mosaic.core.raster.io.RasterIO.{flushAndDestroy, isSameAsRasterParentPath, isSameAsRasterPath, pathAsDataset, writeDatasetToCheckpointDir}
//import com.databricks.labs.mosaic.core.types.model.RasterTile
//import com.databricks.labs.mosaic.utils.{FileUtils, PathUtils, SysUtils}
//import org.gdal.gdal.{Dataset, gdal}
//
//import java.nio.file.{Files, Paths, StandardCopyOption}
//import java.util.UUID
//import scala.util.Try

// TODO 0.4.3 - delete once all is ported from here.


object RasterClassic {

    /////////////////////////////////////////////////////////
    // CHECKPOINT and SOME DRIVER AND SOME DATASET
    /////////////////////////////////////////////////////////
//    /**
//     * If not currently set:
//     *   - will try from driver.
//     *   - will set the found name.
//     *
//     * @return
//     * The raster's driver short name or [[NO_DRIVER]].
//     */
//    def getDriverShortName: String =
//        Try {
//            this.getDriverShortNameOpt match {
//                case Some(name) if name != NO_DRIVER => name
//                case _                                      =>
//                    // try to identify from pseudo path
//                    val _n = Try(RasterIO.identifyDriverFromRawPath(this.identifyPseudoPath))
//                    if (_n.isSuccess) {
//                        this.updateCreateInfoDriver(_n.get)
//                        _n.get
//                    } else {
//                        this.updateCreateInfoDriver(NO_DRIVER)
//                        NO_DRIVER
//                    }
//            }
//        }.getOrElse(NO_DRIVER)
//
//    /** @return whether clean path starts with configured checkpoint dir. */
//    def isCheckpointPath: Boolean = {
//        this.getCleanPath.startsWith(GDAL.getCheckpointDir)
//    }
//
//    def isPathCleanExists: Boolean = Try(Files.exists(Paths.get(getCleanPath))).isSuccess
//
//    def isParentPathCleanExists: Boolean = Try(Files.exists(Paths.get(getCleanParentPath))).isSuccess
//
//    def isSameAsRasterPath(aPath: String, raster: RasterGDAL): Boolean = {
//        raster.getCleanPath == PathUtils.getCleanPath(aPath)
//    }
//
//    def isSameAsRasterParentPath(aPath: String, raster: RasterGDAL): Boolean = {
//        raster.getCleanParentPath == PathUtils.getCleanPath(aPath)
//    }
//
//    /**
//     * Clone existing [[Dataset]] to a new object with a new path.
//     *   - Bad dataset returns None
//     *
//     * @param dataset
//     *   [[Dataset]] to clone.
//     * @param doDestroy
//     *   Whether to destroy the src dataset upon cloning.
//     * @return
//     *   Option (Dataset, Map[String, String] with a new local path and driver.
//     */
//    def cloneDataset(dataset: Dataset, doDestroy: Boolean): Option[(Dataset, Map[String, String])] =
//        Try {
//
//            // make a complete internal copy
//            // we want a local tmp file regardless of how the raster originated
//            val driver = dataset.GetDriver()
//            val driverShortName = driver.getShortName
//            val dstPath = PathUtils.createTmpFilePath(GDAL.getExtension(driverShortName))
//            val dstDataset = driver.CreateCopy(dstPath, dataset, 1)
//            val dstCreateInfo = Map(
//                RASTER_PATH_KEY -> dstPath,
//                RASTER_DRIVER_KEY -> driverShortName
//            )
//
//            // cleanup
//            if (doDestroy) flushAndDestroy(dataset)
//            driver.delete()
//
//            (dstDataset, dstCreateInfo)
//        }.toOption
//
//    /**
//     * Clone existing path for a [[Dataset]] to a new object with a new path.
//     *   - Bad dataset returns None
//     *
//     * @param path
//     *   Path to load as [[Dataset]] to clone.
//     * @param overrideDriverOpt
//     *   Option to specify the driver to use.
//     * @return
//     *   Option (Dataset, Map[String, String] with a new local path and driver.
//     */
//    def cloneDatasetPath(path: String, overrideDriverOpt: Option[String] = None): Option[(Dataset, Map[String, String])] = {
//        val driverShortName = overrideDriverOpt match {
//            case Some(name) => name
//            case _          => this.identifyDriverFromRawPath(path)
//        }
//        val dataset = pathAsDataset(path, Some(driverShortName))
//        cloneDataset(dataset, doDestroy = true)
//    }
//
//    /**
//     * Writes a raster dataset to the configured checkpoint directory.
//     * @param dataset
//     *   The dataset to write (avoid assumptions).
//     * @param doDestroy
//     *   A boolean indicating if the raster object should be destroyed after
//     *   writing.
//     *   - file paths handled separately. Skip deletion of interim file writes,
//     *     if any.
//     * @return
//     *   The path where written (may differ, e.g. due to subdatasets).
//     */
//    def writeDatasetToCheckpointDir(dataset: Dataset, doDestroy: Boolean): String = {
//        val tmpDriver = dataset.GetDriver()
//        val uuid = UUID.randomUUID().toString
//        val ext = GDAL.getExtension(tmpDriver.getShortName)
//        val writePath = s"${getCheckpointDir}/$uuid.$ext"
//        val tmpDs = tmpDriver.CreateCopy(writePath, dataset, 1)
//        tmpDriver.delete()
//        if (tmpDs == null) {
//            val error = gdal.GetLastErrorMsg()
//            throw new Exception(s"Error writing raster dataset to checkpoint dir: $error")
//        } else flushAndDestroy(tmpDs)
//        if (doDestroy) flushAndDestroy(dataset)
//        writePath
//    }

    /////////////////////////////////////////////////////////
    // BULK OF READ / WRITE
    /////////////////////////////////////////////////////////

    //    /**
    //     * Cleans up the raster driver and references.
    //     *   - This will not clean up a file stored in a Databricks location,
    //     *     meaning DBFS, Volumes, or Workspace paths are skipped. Unlinks the
    //     *     raster file. After this operation the raster object is no longer
    //     *     usable. To be used as last step in expression after writing to
    //     *     bytes.
    //     */
    //    @deprecated("0.4.3 recommend to let CleanUpManager handle")
    //    def safeCleanUpPath(aPath: String, raster: RasterGDAL, allowThisPathDelete: Boolean): Unit = {
    //        // 0.4.2 - don't delete any fuse locations.
    //        if (
    //            !PathUtils.isFuseLocation(aPath) && !isSameAsRasterParentPath(aPath, raster)
    //                && (!isSameAsRasterPath(aPath, raster) || allowThisPathDelete)
    //        ) {
    //            Try(gdal.GetDriverByName(raster.getDriverShortName).Delete(aPath))
    //            PathUtils.cleanUpPath(aPath)
    //        }
    //    }
    //
    //    // ////////////////////////////////////////////////////////
    //    // RASTER - WRITE
    //    // ////////////////////////////////////////////////////////
    //
    //    /**
    //     * Writes a raster to a byte array.
    //     *
    //     * @param raster
    //     *   The [[RasterGDAL]] object that will be used in the write.
    //     * @param doDestroy
    //     *   A boolean indicating if the raster object should be destroyed after
    //     *   writing.
    //     *   - file paths handled separately.
    //     * @return
    //     *   A byte array containing the raster data.
    //     */
    //    def writeRasterToBytes(raster: RasterGDAL, doDestroy: Boolean): Array[Byte] = {
    //        // TODO 0.4.3 - this will get refined...
    //        val readPath = {
    //            val tmpPath =
    //                if (raster.isSubDataset) {
    //                    val tmpPath = PathUtils.createTmpFilePath(raster.getExtFromDriver)
    //                    // TODO Subdataset should be Dataset write!
    //                    this.writeRasterToPath(raster, tmpPath, doDestroy = false) // destroy 1x at end
    //                    tmpPath
    //                } else {
    //                    raster.getCleanPath
    //                }
    //            if (Files.isDirectory(Paths.get(tmpPath))) {
    //                val parentDir = Paths.get(tmpPath).getParent.toString
    //                val fileName = Paths.get(tmpPath).getFileName.toString
    //                val prompt = SysUtils.runScript(Array("/bin/sh", "-c", s"cd $parentDir && zip -r0 $fileName.zip $fileName"))
    //                if (prompt._3.nonEmpty) {
    //                    throw new Exception(s"Error zipping file: ${prompt._3}. Please verify that zip is installed. Run 'apt install zip'.")
    //                }
    //                s"$tmpPath.zip"
    //            } else {
    //                tmpPath
    //            }
    //        }
    //        val byteArray = FileUtils.readBytes(readPath)
    //
    //        if (doDestroy) raster.flushAndDestroy()
    //        byteArray
    //    }
    //
    //    /**
    //     * Writes a raster to the configured checkpoint directory.
    //     *
    //     * @param doDestroy
    //     *   A boolean indicating if the raster object should be destroyed after
    //     *   writing.
    //     *   - file paths handled separately. Skip deletion of interim file writes,
    //     *     if any.
    //     * @return
    //     *   The path where written (may differ, e.g. due to subdatasets).
    //     */
    //    def writeRasterToCheckpointDir(raster: RasterGDAL, doDestroy: Boolean): String = {
    //        // TODO 0.4.3 - this will get refined...
    //        if (raster.isCheckpointPath) {
    //            raster.getCleanPath
    //        } else {
    //            if (raster.isSubDataset || !raster.isPathCleanExists) {
    //                writeDatasetToCheckpointDir(raster.getDatasetHydratedOpt().get, doDestroy)
    //            } else {
    //                val thisCleanPath = Paths.get(raster.getCleanPath)
    //                val fromDir = thisCleanPath.getParent
    //                val toDir = GDAL.getCheckpointDir
    //                val stemRegex = PathUtils.getStemRegex(raster.getRawPath)
    //                PathUtils.wildcardCopy(fromDir.toString, toDir, stemRegex)
    //                if (doDestroy) raster.flushAndDestroy()
    //                s"$toDir/${thisCleanPath.getFileName}"
    //            }
    //        }
    //    }
    //
    //    /**
    //     * Writes a raster to a specified file system path.
    //     *
    //     * @param raster
    //     *   The [[RasterGDAL]] object that will be used in the write.
    //     * @param newPath
    //     *   The path to write the raster.
    //     * @param doDestroy
    //     *   A boolean indicating if the raster object should be destroyed after
    //     *   writing.
    //     *   - file paths handled separately.
    //     * @return
    //     *   The path where written (may differ, e.g. due to subdatasets).
    //     */
    //    def writeRasterToPath(raster: RasterGDAL, newPath: String, doDestroy: Boolean): String = {
    //        if (raster.isSubDataset) {
    //            // TODO 0.4.3 - this logic should use `this.writeDatasetToCheckpointDir()` for [sub]dataset
    //            val tmpDriver = raster.getDatasetHydratedOpt().get.GetDriver()
    //            val tmpDs = tmpDriver.CreateCopy(newPath, raster.getDatasetHydratedOpt().get, 1)
    //            tmpDriver.delete()
    //            if (tmpDs == null) {
    //                val error = gdal.GetLastErrorMsg()
    //                throw new Exception(s"Error writing raster to path: $error")
    //            } else flushAndDestroy(tmpDs)
    //            if (doDestroy) raster.flushAndDestroy()
    //            newPath
    //        } else {
    //            // TODO 0.4.3 - this will get refined...
    //            val thisCleanPath = Paths.get(raster.getCleanPath)
    //            val fromDir = thisCleanPath.getParent
    //            val toDir = Paths.get(newPath).getParent
    //            val stemRegex = PathUtils.getStemRegex(raster.getRawPath)
    //            PathUtils.wildcardCopy(fromDir.toString, toDir.toString, stemRegex)
    //            if (doDestroy) raster.flushAndDestroy()
    //            s"$toDir/${thisCleanPath.getFileName}"
    //        }
    //    }
    //
    //    // ////////////////////////////////////////////////////////
    //    // RASTER / BAND - READ
    //    // ////////////////////////////////////////////////////////
    //
    //    /**
    //     * Reads a raster band from a file system path. Reads a subdataset band if
    //     * the path is to a subdataset.
    //     * @example
    //     *   Raster: path = "/path/to/file.tif" Subdataset: path =
    //     *   "FORMAT:/path/to/file.tif:subdataset"
    //     * @param bandIndex
    //     *   The band index to read (1+ indexed).
    //     * @param createInfo
    //     *   Map of create info for the raster.
    //     * @return
    //     *   A [[RasterGDAL]] object.
    //     */
    //    def readBandFrom(bandIndex: Int, createInfo: Map[String, String]): RasterBandGDAL = {
    //        val raster = readRasterFrom(createInfo)
    //        val result = raster.getBand(bandIndex)
    //        flushAndDestroy(raster)
    //
    //        result
    //    }
    //
    //    /**
    //     * Reads a raster from a byte array. Expects "driver" in createInfo.
    //     * @param contentBytes
    //     *   The byte array containing the raster data.
    //     * @param createInfo
    //     *   Creation info of the raster as relating to serialization of
    //     *   [[RasterTile]]. Note: This is not the same as the metadata of the
    //     *   raster. This is not the same as GDAL creation options.
    //     * @return
    //     *   A [[RasterGDAL]] object.
    //     */
    //    def readRasterFrom(contentBytes: Array[Byte], createInfo: Map[String, String]): RasterGDAL = {
    //        // TODO 0.4.3 - this will get refined...
    //        if (Option(contentBytes).isEmpty || contentBytes.isEmpty) {
    //            RasterGDAL(createInfo)
    //        } else {
    //            val memSize = Try(contentBytes.length.toString).getOrElse(-1)
    //            // This is a temp UUID for purposes of reading the raster through GDAL from memory
    //            // The stable UUID is kept in metadata of the raster
    //            val driverSN = createInfo(RASTER_DRIVER_KEY)
    //            val extension = GDAL.getExtension(driverSN)
    //            val tmpPath = PathUtils.createTmpFilePath(extension)
    //            Files.write(Paths.get(tmpPath), contentBytes)
    //            // Try reading as a tmp file, if that fails, rename as a zipped file
    //            val ds = pathAsDataset(tmpPath, Some(driverSN))
    //            if (ds == null) {
    //                val zippedPath = s"$tmpPath.zip"
    //                Files.move(Paths.get(tmpPath), Paths.get(zippedPath), StandardCopyOption.REPLACE_EXISTING)
    //                val readPath = PathUtils.getZipPath(zippedPath)
    //                val ds1 = pathAsDataset(readPath, Some(driverSN))
    //                if (ds1 == null) {
    //                    // the way we zip using uuid is not compatible with GDAL
    //                    // we need to unzip and read the file if it was zipped by us
    //                    val parentDir = Paths.get(zippedPath).getParent
    //                    val prompt = SysUtils.runScript(Array("/bin/sh", "-c", s"cd $parentDir && unzip -o $zippedPath -d $parentDir"))
    //                    // zipped files will have the old uuid name of the raster
    //                    // we need to get the last extracted file name, but the last extracted file name is not the raster name
    //                    // we can't list folders due to concurrent writes
    //                    val lastExtracted = SysUtils.getLastOutputLine(prompt)
    //                    val unzippedPath = PathUtils.parseUnzippedPathFromExtracted(lastExtracted, extension)
    //                    val ds2 = pathAsDataset(unzippedPath, Some(driverSN))
    //                    if (ds2 == null) {
    //                        // TODO: 0.4.3 do we want to just return a tile with error instead of exception?
    //                        throw new Exception(s"Error reading raster from bytes: ${prompt._3}")
    //                    }
    //                    RasterGDAL.createWithDataset(
    //                        ds2,
    //                        createInfo + (
    //                            RASTER_PATH_KEY -> unzippedPath,
    //                            RASTER_MEM_SIZE_KEY -> memSize.toString
    //                        ),
    //                        useCheckpoint = true // path ends up as checkpoint
    //                    )
    //                } else {
    //                    RasterGDAL.createWithDataset(
    //                        ds1,
    //                        createInfo + (
    //                            RASTER_PATH_KEY -> readPath,
    //                            RASTER_MEM_SIZE_KEY -> memSize.toString
    //                        ),
    //                        useCheckpoint = true // path ends up as checkpoint
    //                    )
    //                }
    //            } else {
    //                RasterGDAL.createWithDataset(
    //                    ds,
    //                    createInfo + (
    //                        RASTER_PATH_KEY -> tmpPath,
    //                        RASTER_MEM_SIZE_KEY -> memSize.toString
    //                    ),
    //                    useCheckpoint = true // path ends up as checkpoint
    //                )
    //            }
    //        }
    //    }
    //
    //    /**
    //     * Reads a raster from a file system path. Reads a subdataset if the path
    //     * is to a subdataset.
    //     * @example
    //     *   Raster: path = "/path/to/file.tif" Subdataset: path =
    //     *   "FORMAT:/path/to/file.tif:subdataset"
    //     * @param createInfo
    //     *   Map of create info for the raster.
    //     * @return
    //     *   A [[RasterGDAL]] object.
    //     */
    //    def readRasterFrom(createInfo: Map[String, String]): RasterGDAL = {
    //        // TODO 0.4.3 - this will get refined...
    //        val inPath = createInfo(RASTER_PATH_KEY)
    //        val isSubdataset = PathUtils.isSubdataset(inPath)
    //        val cleanPath = PathUtils.getCleanPath(inPath)
    //        val readPath =
    //            if (isSubdataset) PathUtils.getSubdatasetPath(cleanPath)
    //            else PathUtils.getZipPath(cleanPath)
    //        val ds: Dataset = pathAsDataset(readPath, None)
    //        val error =
    //            if (ds == null) {
    //                val error = gdal.GetLastErrorMsg()
    //                s"""
    //                Error reading raster from path: $readPath
    //                Error: $error
    //            """
    //            } else ""
    //        val driverShortName = Try(ds.GetDriver().getShortName).getOrElse(NO_DRIVER)
    //        // Avoid costly IO to compute MEM size here
    //        // It will be available when the raster is serialized for next operation
    //        // If value is needed then it will be computed when getMemSize is called
    //        // We cannot just use memSize value of the parent due to the fact that the raster could be a subdataset
    //        RasterGDAL.createWithDataset(
    //            ds,
    //            createInfo + (
    //                RASTER_DRIVER_KEY -> driverShortName,
    //                RASTER_LAST_ERR_KEY -> error
    //            ),
    //            useCheckpoint = true
    //        )
    //    }

    //    /** @return Returns file extension. default [[NO_EXT]]. */
    //    def getExtFromDriver: String =
    //        Try {
    //            RasterIO.identifyExtFromDriver(this.getDriverShortName)
    //        }.getOrElse(NO_EXT)

    /////////////////////////////////////////////////
    // HALF-BAKED COPY/PASTE
    /////////////////////////////////////////////////

    //    override def setDataset(dataset: Dataset, useCheckpoint: Boolean): Unit = {
    //        this.flushAndDestroy()
    //        var newCreateInfo = Map.empty[String, String]
    //        Option(dataset) match {
    //            case Some(ds) =>
    //                val driver = ds.GetDriver()
    //                newCreateInfo += (RASTER_DRIVER_KEY -> driver.getShortName)
    //                if (useCheckpoint) {
    //                    val checkPath = RasterIO.writeDatasetToCheckpointDir(ds, doDestroy = false)
    //                    newCreateInfo += (RASTER_PATH_KEY -> checkPath, RASTER_PARENT_PATH_KEY -> checkPath)
    //                }
    //
    //                driver.delete()
    //            case _        => ()
    //        }
    //        this.updateCreateInfo(newCreateInfo)
    //        this.datasetOpt = Option(dataset)
    //
    //        this.resetFlags() // no more handling to be done
    //    }

    //    val result = fusePathOpt match {
    //        case Some(fuseGDAL) if (fusePat =>
    //        // (2a) fusePathOpt is set.
    //        // TODO
    //        case _ =>
    //            createInfo.get(RASTER_PATH_KEY) match {
    //                case Some(localPath) =>
    //                    // (2b) path set
    //                    fuseDirOpt match {
    //                        // TODO test this first as if it is different than fuse path parent,
    //                        //      then need to write to the new dir and abandon current fuse path
    //                        case Some(fuseDir) =>
    //                        // (2b1) use the override dir
    //                        // TODO
    //                        case _ =>
    //                        // (2b2) use the configured checkpoint
    //                        // TODO
    //                    }
    //                case _ => ()
    //                    // (2c) path not set - out of options
    //                    datasetOpt = None
    //            }


    // TODO: this will handle everything
    // (2) Proceed with the following steps:
    //   (a) fuseGDAL set but no longer exists (try to re-use it)
    //   (b) Path set and exists (try to write to fuse)
    //   (c) No other options

    //        if (initDatasetFlag) {
    //            // handle initializing the internal dataset (1x unless `setDataset` called)
    //            // - nothing can be done if this fails unless
    //            //   updates are made, e.g. to set a new path or driver.
    //            if (destroyFlag) this.flushAndDestroy()
    //            if (!this.isDatasetHydrated) {
    //                // focus on loading from path
    //                this.datasetOpt = Try(RasterIO.pathAsDataset(this.getRawPath, this.getDriverShortNameOpt)).toOption
    //            }
    //        } else if (this.isDatasetRefreshFlag) {
    //            // handle any subsequent changes flagged
    //            // - e.g. if destroy was called
    //            //   or path and/or driver changed
    //            if (!destroyFlag) this.flushAndDestroy()
    //            // focus on loading from path
    //            this.datasetOpt = Try(RasterIO.pathAsDataset(this.getRawPath, this.getDriverShortNameOpt)).toOption
    //        }
    //        this.resetFlags()
    //
    //        datasetOpt


}
