package com.databricks.labs.mosaic.core.raster.gdal

import com.databricks.labs.mosaic.{NO_DRIVER, RASTER_BAND_INDEX_KEY, RASTER_DRIVER_KEY, RASTER_PARENT_PATH_KEY, RASTER_PATH_KEY, RASTER_SUBDATASET_NAME_KEY}
import com.databricks.labs.mosaic.core.raster.io.RasterIO
import org.gdal.gdal.{Dataset, gdal}

import scala.util.Try

/**
  * When a [[Dataset]] has been constructed, we need to maintain a few pieces of intformation.
  * - This class allows us to maintain the details even after flushAndDestroy has been called.
  * @param dataset
  *   Defaults to null.
  */
case class DatasetGDAL(var dataset: Dataset = null) {

    // This is set 1x then can be updated.
    // - all the path related functions are
    //   consolidated under this object.
    val pathGDAL = PathGDAL()

    var driverNameOpt: Option[String] = None
    var bandIdxOpt: Option[Int]       = None

    /** @return Has the Dataset ever been hydrated? */
    private var everHydratedFlag: Boolean = false
    def everHydrated: Boolean = everHydratedFlag

    /** @return `createInfo` populated (includes 'parentPath' set to 'path'). */
    def asCreateInfo: Map[String, String] = {
        Map(
            RASTER_PATH_KEY -> pathGDAL.path, // <- pathGDAL
            RASTER_DRIVER_KEY -> driverNameOpt.getOrElse(NO_DRIVER),
            RASTER_PARENT_PATH_KEY -> pathGDAL.path, // <- pathGDAL (also)
            RASTER_SUBDATASET_NAME_KEY -> pathGDAL.getSubdatasetNameOpt.getOrElse(""),
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

    /**
     * `flushAndDestroy` sets to null.
     * @return Is the Dataset non-null?
     */
    def isHydrated: Boolean = {
        val result: Boolean = dataset != null
        if (!everHydratedFlag && result) everHydratedFlag = true
        result
    }

        /**
         * Writes (via driver copy) a raster to a specified file system path.
         *   - Use this for subdataasets or rasters with dataset hydrated.
         *
         * @param newPath
         *   The path to write the raster.
         * @param doDestroy
         *   A boolean indicating if the raster object should be destroyed after
         *   writing.
         *   - file paths handled separately.
         * @return
         *   whether successful write or not
         */
        def datasetCopyToPath(newPath: String, doDestroy: Boolean): Boolean =
            Try {
                this.getDatasetOpt match {
                    case Some(dataset) =>
                        // (1) have hydrated raster
                        val tmpDriver = dataset.GetDriver()
                        try {
                            val tmpDs = tmpDriver.CreateCopy(newPath, dataset, 1)
                            if (tmpDs == null) {
                                // val error = gdal.GetLastErrorMsg()
                                // throw new Exception(s"Error writing raster to path: $error")
                                false
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
                        //  (2) cannot do anything without a hydrated raster
                        false
                }
            }.getOrElse(false)

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
        this.dataset = dataset
        if (this.isHydrated && doUpdateDriver) {
           this.updateDriverName(
               RasterIO.identifyDriverNameFromDataset(dataset))
        } else if (doUpdateDriver) {
            this.updateDriverName(NO_DRIVER)
        }
        this
    }

    /** fluent update, return [[DatasetGDAL]] this. */
    def updateBandIdx(idx: Int): DatasetGDAL = {
        if (idx < 1) bandIdxOpt = None
        else bandIdxOpt = Option(idx)
        this
    }

    /** fluent update, return [[DatasetGDAL]] this. */
    def updateDriverName(name: String): DatasetGDAL = {
        if (name == null || name == NO_DRIVER) driverNameOpt = None
        else driverNameOpt = Option(name)
        this
    }

    /** fluent update, return [[DatasetGDAL]] this. Tries to auto-update subdataset name as well. */
    def updatePath(path: String): DatasetGDAL = {
        pathGDAL.updatePath(path)
        this
    }

}

object DatasetGDAL {

    /**
     * Constructor for unhydrated (no [[Dataset]] initially.
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
}
