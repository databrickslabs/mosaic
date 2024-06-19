package com.databricks.labs.mosaic.core.raster.io

import java.util.{Vector => JVector}
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants.GA_ReadOnly

trait RasterHydrator {

    /** @return Underlying GDAL raster dataset object, hydrated if possible. */
    def getDatasetHydrated: Dataset

    /** rehydrate the underlying GDAL raster dataset object. This is for forcing a refresh. */
    def reHydrate(): Unit
}

/** singleton */
object RasterHydrator {

    /**
     * Opens a raster from a file system path with a given driver.
     * @param path
     *   The path to the raster file.
     * @param driverShortNameOpt
     *   The driver short name to use. If None, then GDAL will try to identify
     *   the driver from the file extension
     * @return
     *   A GDAL [[Dataset]] object.
     */
    def pathAsDataset(path: String, driverShortNameOpt: Option[String]): Dataset = {
        driverShortNameOpt match {
            case Some(driverShortName) =>
                val drivers = new JVector[String]()
                drivers.add(driverShortName)
                gdal.OpenEx(path, GA_ReadOnly, drivers)
            case None                  => gdal.Open(path, GA_ReadOnly)
        }
    }

}
