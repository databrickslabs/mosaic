package com.databricks.labs.mosaic.core.raster.gdal

import com.databricks.labs.mosaic.{
    BAND_META_GET_KEY,
    BAND_META_SET_KEY,
    NO_DRIVER,
    NO_PATH_STRING,
    RASTER_BAND_INDEX_KEY,
    RASTER_CORE_KEYS,
    RASTER_DRIVER_KEY,
    RASTER_PARENT_PATH_KEY,
    RASTER_PATH_KEY,
    RASTER_SUBDATASET_NAME_KEY
}
import com.databricks.labs.mosaic.core.raster.io.RasterIO
import com.databricks.labs.mosaic.functions.ExprConfig
import com.databricks.labs.mosaic.utils.PathUtils
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

    var dsErrFlag = false

    /** @return Has the Dataset ever been hydrated? */
    private var everHydratedFlag: Boolean = false
    def everHydrated: Boolean = everHydratedFlag

    /**
     * Blends from pathGDAL, parentPathGDAL, and `this` DatasetGDAL.
     * - The master view of createInfo.
     *
     * @param includeExtras
     *   Whether to include the createInfoExtras.
     *   If true, the base keys are added second to ensure they are used over extras.
     * @return blended `createInfo` Map populated; optionally include extras.
     */
    def asCreateInfo(includeExtras: Boolean): Map[String, String] = {
        val baseMap = Map(
            RASTER_PATH_KEY -> pathGDAL.path,                 // <- pathGDAL (path as set)
            RASTER_PARENT_PATH_KEY -> parentPathGDAL.path,    // <- parentPathGDAL (path as set)
            RASTER_DRIVER_KEY -> this.getDriverName,          // <- driverName (Dataset, path, + parent path tested)
            RASTER_SUBDATASET_NAME_KEY -> this.getSubsetName, // <- ultimately stored in pathGDAL (set or path)
            RASTER_BAND_INDEX_KEY ->
                bandIdxOpt.getOrElse(-1).toString             // <- stored in `this`
        )
        if (includeExtras) this.createInfoExtras ++ baseMap   // <- second overrides first
        else baseMap
    }
    private var createInfoExtras = Map.empty[String, String]

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

    /** Getter, may be an empty Map. */
    def getCreateInfoExtras: Map[String, String] = this.createInfoExtras

    /** Getter, None if null. */
    def getDatasetOpt: Option[Dataset] = Option(this.dataset)

    /** Getter, defaults to [[NO_DRIVER]]. */
    def getDriverName: String = {
        driverNameOpt match {
            case Some(d) => d
            case _ =>
                if (pathGDAL.hasPathDriverName) pathGDAL.getPathDriverName
                else if (parentPathGDAL.hasPathDriverName) parentPathGDAL.getPathDriverName
                else NO_DRIVER
        }
    }

    /** Getter for option, defaults to None. */
    def getDriverNameOpt: Option[String] = {
        val dn = getDriverName
        if (dn != NO_DRIVER) Some(dn)
        else None
    }

    /** Getter, defaults to [[NO_PATH_STRING]]. */
    def getPath: String = pathGDAL.getPathOpt.getOrElse(NO_PATH_STRING)

    /** Getter, defaults to [[NO_PATH_STRING]]. */
    def getParentPath: String = parentPathGDAL.getPathOpt.getOrElse(NO_PATH_STRING)

    /** @return get subdataset name (stored in pathGDAL, default is ""). */
    def getSubsetName: String = pathGDAL.getSubsetName

    /** @return get subdataset name option (stored in pathGDAL, default is None). */
    def getSubNameOpt: Option[String] = pathGDAL.getSubNameOpt

    /**
     * `flushAndDestroy` sets to null.
     * @return Is the Dataset non-null?
     */
    def isHydrated: Boolean = {
        val result: Boolean = dataset != null
        if (!everHydratedFlag && result) everHydratedFlag = true
        result
    }

    /** @return whether subdataset has been set (stored in pathGDAL). */
    def isSubdataset: Boolean = pathGDAL.isSubdataset

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
            Files.createDirectories(Paths.get(newDir)) // <- (just in case)
            val newPathOpt: Option[String] = this.getDatasetOpt match {
                case Some(_) if !pathGDAL.isSubdataset && !pathGDAL.isPathZip =>
                    // (1a) try copy from dataset to a new path
                    val ext = RasterIO.identifyExtFromDriver(this.getDriverName)
                    val newFN = this.pathGDAL.getFilename
                    val newPath = s"$newDir/$newFN"
                    if (datasetCopyToPath(newPath, doDestroy = doDestroy, skipUpdatePath = true)) {
                        Some(newPath)
                    } else if (pathGDAL.isPathSetAndExists) {
                        // (1b) try file copy from path to new dir
                        pathGDAL.rawPathWildcardCopyToDir(newDir, skipUpdatePath = true)
                    } else {
                        // (1c) unsuccessful
                        dsErrFlag = true
                        None // <- unsuccessful
                    }
                case _ if pathGDAL.isPathSetAndExists =>
                    // (2a) try file copy from path
                    pathGDAL.rawPathWildcardCopyToDir(newDir, skipUpdatePath = true)
                case _ =>
                    dsErrFlag = true
                    None // <- (4) unsuccessful
            }

            if (!skipUpdatePath) {
                newPathOpt match {
                    case Some(newPath) =>
                        this.updatePath(newPath)
                    case _ => () // <- do nothing ???
                }
            }

            newPathOpt
        }.getOrElse{
            dsErrFlag = true
            None // <- unsuccessful
        }

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
            val success = this.getDatasetOpt match {
                case Some(ds) =>
                    // (1) have hydrated tile
                    val tmpDriver = ds.GetDriver()
                    try {
                        val tmpDs = tmpDriver.CreateCopy(newPath, ds)
                        if (tmpDs == null) {
                            dsErrFlag = true
                            false // <- unsuccessful
                        } else {
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
        if (aPathGDAL.isSubdataset && aPathGDAL.getSubsetName == subsetName) {
            // this already is the subdataset
            // copy considered, but not clear that is needed
            this
        } else {
            // not already the subset asked for
            val basePathGDAL =
                if (!aPathGDAL.isSubdataset) {
                    aPathGDAL
                } else {
                    // make sure we are using the base path, not the subdataset path
                    val p = PathUtils.getCleanPath(
                        aPathGDAL.path,
                        addVsiZipToken = aPathGDAL.isPathZip,
                        uriGdalOpt = aPathGDAL.getRawUriGdalOpt
                    )
                    PathGDAL(p)
                }

            Try(subdatasets(basePathGDAL)(s"${subsetName}_tmp")).toOption match {
                case Some(sPathRaw) =>
                    // (1) found the subdataset in the metadata
                    // - need to clean that up though to actually load
                    val loadPathGDAL = PathGDAL(sPathRaw)
                    val loadPath = loadPathGDAL.asGDALPathOpt(getDriverNameOpt).get
                    // (2) use the subdataset in the path vs the option
                    RasterIO.rawPathAsDatasetOpt(loadPath, subNameOpt = None, getDriverNameOpt, exprConfigOpt) match {
                        case Some(ds) =>
                            // (3) subset loaded
                            val result = DatasetGDAL()
                            result.updatePath(sPathRaw)
                            result.updateSubsetName(subsetName)
                            result.updateDataset(ds, doUpdateDriver = true)
                            result
                        case _ =>
                            // (4) subset not loaded
                            val result = DatasetGDAL()
                            result.dsErrFlag = true
                            result.updatePath(sPathRaw)
                            result.updateDriverName(getDriverName)
                            result
                    }
                case _ =>
                    // (5) subset not found
                    val result = DatasetGDAL()
                    result.dsErrFlag = true
                    result
            }
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

        // get the path (no subdataset)
        val gdalPath = aPathGDAL.asGDALPathOptNoSubName(driverNameOpt).get

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

    /////////////////////////////////
    // CREATE INFO UPDATE FUNCTIONS
    /////////////////////////////////

    /** createInfo "Extras" Map back to empty, return `this` (fluent). */
    def resetCreateInfoExtras: DatasetGDAL = {
        createInfoExtras = Map.empty[String, String]
        this
    }

    /**
     * Set the dataset, update the driver if directed.
     * - may be null but recommend `flushAndDestroy` for that.
     * @param dataset
     *   [[Dataset]] to update.
     * @param doUpdateDriver
     *   Whether to update `driverName` or keep current
     * @return
     */
    def updateDataset(dataset: Dataset, doUpdateDriver: Boolean): DatasetGDAL = {
        this.flushAndDestroy()
        this.dataset = dataset
        if (this.isHydrated && doUpdateDriver) {
            this.updateDriverName(
                RasterIO.identifyDriverNameFromDataset(this.dataset))
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

    /** fluent update, return [[DatasetGDAL]] this, (stores the value in pathGDAL). */
    def updateSubsetName(subsetName: String): DatasetGDAL = {
        pathGDAL.updateSubsetName(subsetName)
        this
    }

    /** fluent update, return [[DatasetGDAL]] this, (stores the values). */
    def updateCreateInfo(createInfo: Map[String, String], extrasOnly: Boolean = false): DatasetGDAL = {
        this.resetCreateInfoExtras
        for ((key, value) <- createInfo) {
            this.updateCreateInfoEntry(key, value, extrasOnly = extrasOnly)
        }
        this
    }

    /** fluent update, return [[DatasetGDAL]] this, (stores the key / value). */
    def updateCreateInfoEntry(key: String, value: String, extrasOnly: Boolean): DatasetGDAL = {
        val isCoreKey = RASTER_CORE_KEYS.contains(key)
        if (isCoreKey && !extrasOnly) {
            // update core key
            if (key == RASTER_PATH_KEY) this.updatePath(value)
            else if (key == RASTER_PARENT_PATH_KEY) this.updateParentPath(value)
            else if (key == RASTER_DRIVER_KEY) this.updateDriverName(value)
            else if (key == RASTER_SUBDATASET_NAME_KEY) this.updateSubsetName(value)
            else if (key == RASTER_BAND_INDEX_KEY) this.updateBandIdx(Try(value.toInt).getOrElse(-1))
        } else if (!isCoreKey) {
            // update "extra" key
            // - could be last cmd, error, memsize, all parents
            // - could be other ad-hoc keys as well
            this.createInfoExtras += (key -> value)
        }
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
        result.updateDriverName(driverName)

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

        result
    }

    /**
     * Constructor for un-hydrated (no [[Dataset]] initially.
     * - Uses the provided createInfo.
     *
     * @param createInfo
     * @return [[DatasetGDAL]]
     */
    def apply(createInfo: Map[String, String]): DatasetGDAL = {
        val result = DatasetGDAL()
        result.updateCreateInfo(createInfo, extrasOnly = false)

        result
    }
}
