package com.databricks.labs.mosaic.core.raster.gdal

import com.databricks.labs.mosaic.{BAND_META_GET_KEY, BAND_META_SET_KEY, NO_DRIVER, NO_PATH_STRING, RASTER_BAND_INDEX_KEY, RASTER_DRIVER_KEY, RASTER_PARENT_PATH_KEY, RASTER_PATH_KEY, RASTER_SUBDATASET_NAME_KEY}
import com.databricks.labs.mosaic.core.raster.io.RasterIO
import com.databricks.labs.mosaic.functions.ExprConfig
import org.gdal.gdal.Dataset

import java.nio.file.{Files, Paths}
import java.util.Locale
import scala.collection.JavaConverters.dictionaryAsScalaMapConverter
import scala.util.Try

/**
  * When a [[Dataset]] has been constructed, we need to maintain a few pieces of intformation.
  * - This class allows us to maintain the details even after flushAndDestroy has been called.
  */
case class DatasetGDAL() {

    // set by `updateDataset`
    var dataset: Dataset = null

    // set by `updateDataset and/or `updateDriverName`
    var driverNameOpt: Option[String] = None

    // path set by `updatePath`
    // - all the path related functions are
    //   consolidated under this object.
    val pathGDAL = PathGDAL()

    val parentPathGDAL = PathGDAL()

    // set by `updateBandIdx`, access directly.
    var bandIdxOpt: Option[Int] = None

    // set by updateSubdatasetName, access directly.
    var subdatasetNameOpt: Option[String] = None

    var dsErrFlag = false

    /** @return Has the Dataset ever been hydrated? */
    private var everHydratedFlag: Boolean = false
    def everHydrated: Boolean = everHydratedFlag

    /** @return `createInfo` populated (doesn't set parent path). */
    def asCreateInfo: Map[String, String] = {
        Map(
            RASTER_PATH_KEY -> pathGDAL.path, // <- pathGDAL
            RASTER_PARENT_PATH_KEY -> parentPathGDAL.path, // <- parentPathGDAL
            RASTER_DRIVER_KEY -> driverNameOpt.getOrElse(NO_DRIVER),
            RASTER_SUBDATASET_NAME_KEY -> this.subdatasetNameOpt.getOrElse(""),
            RASTER_BAND_INDEX_KEY -> bandIdxOpt.getOrElse(-1).toString
        )
    }

    /**
     * Flush and destroy this dataset, if it exists.
     * Dataset is also set to null for clarity as it is no longer useful.
     * @return
     *   Return `this` [[DatasetGDAL]] object (fluent).
     */
    def flushAndDestroy(): DatasetGDAL = {
        RasterIO.flushAndDestroy(dataset)
        this.dataset = null
        this
    }

    /** Getter, None if null. */
    def getDatasetOpt: Option[Dataset] = Option(this.dataset)

    /** Getter, defaults to [[NO_DRIVER]]. */
    def getDriverName: String = driverNameOpt.getOrElse(NO_DRIVER)

    /** Getter, defaults to [[NO_PATH_STRING]]. */
    def getPath: String = pathGDAL.getPathOpt.getOrElse(NO_PATH_STRING)

    /** Getter, defaults to [[NO_PATH_STRING]]. */
    def getParentPath: String = parentPathGDAL.getPathOpt.getOrElse(NO_PATH_STRING)

    /**
     * `flushAndDestroy` sets to null.
     * @return Is the Dataset non-null?
     */
    def isHydrated: Boolean = {
        val result: Boolean = dataset != null
        if (!everHydratedFlag && result) everHydratedFlag = true
        result
    }

    //scalastyle:off println
    /**
     * Writes a tile to a specified file system directory:
     *   - if dataset hydrated and not a subdataset, then use `datasetCopyToPath`.
     *   - otherwise, if the path is set and exists, use `rawPathWildcardCopyToDir`.
     *
     * @param newDir
     *   Provided directory.
     * @param doDestroy
     *   Whether to destroy `this` dataset after copy.
     * @param skipUpdatePath
     *   Whether to update the path on [[PathGDAL]].
     * @return
     *   Option string with the new path (vs dir), None if unable to copy.
     */
    def datasetOrPathCopy(newDir: String, doDestroy: Boolean, skipUpdatePath: Boolean): Option[String] =
        Try {
            //println("::: datasetOrPathCopy :::")
            Files.createDirectories(Paths.get(newDir)) // <- (just in case)
            //println(s"... pathGDAL isPathZip? ${pathGDAL.isPathZip}")
            val newPathOpt: Option[String] = this.getDatasetOpt match {
                case Some(_) if !pathGDAL.isSubdatasetPath && !pathGDAL.isPathZip =>
                    // (1a) try copy from dataset to a new path
                    val ext = RasterIO.identifyExtFromDriver(getDriverName)
                    val newFN = this.pathGDAL.getFilename
                    val newPath = s"$newDir/$newFN"
                    //println(s"... DatasetGDAL - attempting dataset copy for newDir '$newPath'")
                    if (datasetCopyToPath(newPath, doDestroy = doDestroy, skipUpdatePath = true)) {
                        Some(newPath)
                    } else if (pathGDAL.isPathSetAndExists) {
                        // (1b) try file copy from path to new dir
                        //println(s"... DatasetGDAL - after dataset - attempting wildcard copy for newDir '$newDir'")
                        pathGDAL.rawPathWildcardCopyToDir(newDir, skipUpdatePath = true)
                    } else {
                        // (1c) unsuccessful
                        //println(s"... DatasetGDAL - UNSUCCESSFUL (after dataset and path attempt)")
                        dsErrFlag = true
                        None // <- unsuccessful
                    }
                case _ if pathGDAL.isPathSetAndExists =>
                    // (2a) try file copy from path
                    //println(s"... DatasetGDAL - attempting copy (+ wildcard) for newDir '$newDir'")
                    pathGDAL.rawPathWildcardCopyToDir(newDir, skipUpdatePath = true)
                case _ =>
                    //println(s"... DatasetGDAL - NO DATASET OR PATH TO COPY")
                    dsErrFlag = true
                    None // <- (4) unsuccessful
            }

            if (!skipUpdatePath) {
                newPathOpt match {
                    case Some(newPath) =>
                        this.updatePath(newPath)
                    case _ => ()
                }
            }

            newPathOpt
        }.getOrElse{
            //println(s"... DatasetGDAL - EXCEPTION - NO DATASET OR PATH TO COPY")
            dsErrFlag = true
            None // <- unsuccessful
        }
    //scalastyle:on println

    //scalastyle:off println
    /**
     * Writes (via driver copy) a tile to a specified file system path.
     *   - Use this for non-subdataaset rasters with dataset hydrated.
     *
     * @param newPath
     *   The path to write the tile.
     * @param doDestroy
     *   A boolean indicating if the tile object should be destroyed after
     *   writing.
     *   - file paths handled separately.
     * @param skipUpdatePath
     *   Whether to update the path on [[PathGDAL]].
     * @return
     *   whether successful write or not
     */
    def datasetCopyToPath(newPath: String, doDestroy: Boolean, skipUpdatePath: Boolean): Boolean =
        Try {
            //println("::: datasetCopyToPath :::")
            val success = this.getDatasetOpt match {
                case Some(ds) =>
                    // (1) have hydrated tile
                    val tmpDriver = ds.GetDriver()
                    try {
                        //println(s"...driver null? ${tmpDriver == null}")
                        //Try(println(s"...driver name? ${tmpDriver.getShortName}"))
                        val tmpDs = tmpDriver.CreateCopy(newPath, ds)

                        if (tmpDs == null) {
                            //println(s"...ds null for new path '$newPath'")
                            dsErrFlag = true
                            false // <- unsuccessful
                        } else {
                            //println(s"...ds copied to new path '$newPath'")
                            // - destroy the temp [[Dataset]]
                            // - if directed, destroy this [[Dataset]]
                            RasterIO.flushAndDestroy(tmpDs)
                            if (doDestroy) this.flushAndDestroy()
                            true
                        }
                    } finally {
                        tmpDriver.delete()
                    }
                case _             =>
                    //  (2) cannot do anything without a hydrated tile
                    dsErrFlag = true
                    false // <- unsuccessful
            }

            if (!skipUpdatePath) {
                this.updatePath(newPath)
            }

            success
        }.getOrElse{
            dsErrFlag = true
            false // <- unsuccessful
        }
    //scalastyle:on println

    /**
     * Get a particular subdataset by name.
     * @param aPathGDAL
     *   The [[PathGDAL]] to use.
     * @param subsetName
     *   The name of the subdataset to get.
     * @param exprConfigOpt
     *   Option [[ExprConfig]].
     * @return
     *   New [[DatasetGDAL]].
     */
    def getSubdatasetObj(aPathGDAL: PathGDAL, subsetName: String, exprConfigOpt: Option[ExprConfig]): DatasetGDAL = {

        Try(subdatasets(aPathGDAL)(s"${subsetName}_tmp")).toOption match {
            case Some(sPathRaw) =>
                // (1) found the subdataset
                RasterIO.rawPathAsDatasetOpt(sPathRaw, driverNameOpt, exprConfigOpt) match {
                    case Some(ds) =>
                        // (2) was able to load the subdataset
                        val result = DatasetGDAL()
                        result.updatePath(sPathRaw)
                        result.updateSubdatasetName(subsetName)
                        result.updateDataset(ds, doUpdateDriver = true)
                        result
                    case _ =>
                        // (3) wasn't able to load the subdataset
                        val result = DatasetGDAL()
                        result.dsErrFlag = true
                        result.updatePath(sPathRaw)
                        result.updateDriverName(getDriverName)
                        result
                }
            case _ =>
                // (4) didn't find the subdataset
                val result = DatasetGDAL()
                result.dsErrFlag = true
                result
        }
    }

    /**
     * Get a particular subdataset by name.
     * - This converts to [[PathGDAL]] and calls the other signature.
     *
     * @param aPath
     *   The path to the parent (full) file.
     * @param subsetName
     *   The name of the subdataset to get.
     * @param exprConfigOpt
     *   Option [[ExprConfig]].
     * @return
     *   New [[DatasetGDAL]].
     */
    def getSubdatasetObj(aPath: String, subsetName: String, exprConfigOpt: Option[ExprConfig]): DatasetGDAL = {
        val uriDeepCheck = Try(exprConfigOpt.get.isUriDeepCheck).getOrElse(false)
        val aPathGDAL = PathGDAL(path = aPath, uriDeepCheck)
        getSubdatasetObj(aPathGDAL, subsetName, exprConfigOpt)
    }

    /** @return Returns the tile's metadata as a Map, defaults to empty. */
    def metadata: Map[String, String] = {
        Option(dataset.GetMetadataDomainList())
            .map (_.toArray)
            .map (domain =>
                domain
                    .map (domainName =>
                        Option (dataset.GetMetadata_Dict (domainName.toString) )
                            .map (_.asScala.toMap.asInstanceOf[Map[String, String]] )
                            .getOrElse (Map.empty[String, String] )
                    )
                    .reduceOption (_++ _)
                    .getOrElse(Map.empty[String, String])
            ).getOrElse(Map.empty[String, String])
    }

    /** @return Returns a tile's subdatasets as a Map using provided [[PathGDAL]], default empty. */
    def subdatasets(aPathGDAL: PathGDAL): Map[String, String] =
    Try {
        val dict = Try(dataset.GetMetadata_Dict("SUBDATASETS"))
            .getOrElse(new java.util.Hashtable[String, String]())
        val subdatasetsMap = Option(dict)
            .map(_.asScala.toMap.asInstanceOf[Map[String, String]])
            .getOrElse(Map.empty[String, String])
        val keys = subdatasetsMap.keySet

        val gdalPath = aPathGDAL.asGDALPathOpt.get

        keys.flatMap(key =>
            if (key.toUpperCase(Locale.ROOT).contains("NAME")) {
                val path = subdatasetsMap(key)
                val pieces = path.split(":")
                Seq(
                    key -> pieces.last,
                    s"${pieces.last}_tmp" -> path,
                    pieces.last -> s"${pieces.head}:$gdalPath:${pieces.last}"
                )
            } else Seq(key -> subdatasetsMap(key))
        ).toMap
    }.getOrElse(Map.empty[String, String])

    /**
     * This converts path to [[PathGDAL]].
     *
     * @param aPath
     * @param exprConfigOpt
     * @return Returns a tile's subdatasets as a Map, default empty.
     */
    def subdatasets(aPath: String, exprConfigOpt: Option[ExprConfig]): Map[String, String] = {
        val uriDeepCheck = Try(exprConfigOpt.get.isUriDeepCheck).getOrElse(false)
        val aPathGDAL = PathGDAL(path = aPath, uriDeepCheck)
        subdatasets(aPathGDAL)
    }

    /**
     * Key for band number assembled and attempted.
     * - This is the metadata key managed by Mosaic.
     * - So becomes <globalPrefix>#GDAL_MOSAIC_BAND_INDEX
     * - For NetCDF this is "NC_GLOBAL#GDAL_MOSAIC_BAND_INDEX"
     * - [[BAND_META_GET_KEY]] is has "GDAL_" pre-pended, where
     *   [[BAND_META_SET_KEY]] does not.
     *
     * @globalPrefix
     *   E.g. "NC_GLOBAL" (based on dataset).
     * @return value for [[BAND_META_GET_KEY]] if found in metadata; otherwise -1.
     */
    def tryBandNumFromMetadata(globalPrefix: String): Int =
        Try {
            tryKeyFromMetadata(s"$globalPrefix#$BAND_META_GET_KEY").toInt
        }.getOrElse(-1)

    /** @return value for key if found in metadata; otherwise "". */
    def tryKeyFromMetadata(key: String): String = Try {
        dataset.GetMetadataItem(key)
    }.getOrElse("")

    /** Set a metadata key, if dataset hydrated; return `this` (fluent). */
    def setMetaKey(key: String, value: String): DatasetGDAL =
        Try {
            dataset.SetMetadataItem(key, value)
            this
        }.getOrElse(this)

    /**
     * Set the dataset, update the driver if directed.
     * - may be null but recommend `flushAndDestroy` for that.
     * @param dataset
     *   [[Dataset]] to update.
     * @param doUpdateDriver
     *   Whether to upate `driverName`, if dataset is null, falls back to [[NO_DRIVER]]
     * @return
     */
    def updateDataset(dataset: Dataset, doUpdateDriver: Boolean): DatasetGDAL = {
        this.flushAndDestroy()
        this.dataset = dataset
        if (this.isHydrated && doUpdateDriver) {
            this.updateDriverName(
                RasterIO.identifyDriverNameFromDataset(this.dataset))
        } else if (doUpdateDriver) {
            this.updateDriverName(NO_DRIVER)
            this.dsErrFlag = true
        }
        this
    }

    /** fluent update, return [[DatasetGDAL]] this (also sets metadata key). */
    def updateBandIdx(idx: Int): DatasetGDAL = {
        if (idx < 1) bandIdxOpt = None
        else bandIdxOpt = Option(idx)
        this.setMetaKey(BAND_META_SET_KEY, idx.toString)
        this
    }

    /** fluent update, return [[DatasetGDAL]] this. */
    def updateDriverName(name: String): DatasetGDAL = {
        if (name == null || name == NO_DRIVER) driverNameOpt = None
        else driverNameOpt = Option(name)
        this
    }

    /** fluent update, return [[DatasetGDAL]] this. */
    def updatePath(path: String): DatasetGDAL = {
        pathGDAL.updatePath(path)
        this
    }

    /** fluent update, return [[DatasetGDAL]] this. */
    def updateParentPath(parentPath: String): DatasetGDAL = {
        parentPathGDAL.updatePath(parentPath)
        this
    }

    /** fluent update, return [[DatasetGDAL]] this, (simple setter, only stores the value). */
    def updateSubdatasetName(subsetName: String): DatasetGDAL = {
        subdatasetNameOpt = Option(subsetName)
        this
    }

}

object DatasetGDAL {

    /**
     * Constructor for un-hydrated (no [[Dataset]] initially.
     * - Sets the driver based on provided, if valid;
     *   otherwise based on path, if possible.
     *
     * @param path
     * @param driverName
     * @return [[DatasetGDAL]]
     */
    def apply(path: String, driverName: String): DatasetGDAL = {
        val result = DatasetGDAL()
        result.updatePath(path)
        if (driverName != NO_DRIVER) result.updateDriverName(driverName)
        else result.updateDriverName(result.pathGDAL.getPathDriverName)


        result
    }

    /**
     * Constructor for un-hydrated (no [[Dataset]] initially.
     * - Sets the driver based on path, if possible.
     *
     * @param path
     * @return [[DatasetGDAL]]
     */
    def apply(path: String): DatasetGDAL = {
        val result = DatasetGDAL()
        result.updatePath(path)
        result.updateDriverName(result.pathGDAL.getPathDriverName)

        result
    }
}
