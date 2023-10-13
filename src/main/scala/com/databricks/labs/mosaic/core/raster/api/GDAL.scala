package com.databricks.labs.mosaic.core.raster.api

import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL

import org.gdal.gdal.gdal

/**
 * GDAL Raster API. It uses [[MosaicRasterGDAL]] as the [[RasterReader]].
 */
object GDAL extends RasterAPI(MosaicRasterGDAL) {

    override def name: String = "GDAL"

    /**
     * Enables GDAL on the worker nodes. GDAL requires drivers to be
     * registered on the worker nodes. This method registers all the
     * drivers on the worker nodes.
     */
    override def enable(): Unit = {
        gdal.UseExceptions()
        if (gdal.GetDriverCount() == 0) gdal.AllRegister()
    }

    override def getExtension(driverShortName: String): String = {
        val driver = gdal.GetDriverByName(driverShortName)
        driver.GetMetadataItem("DMD_EXTENSION")
    }

}
