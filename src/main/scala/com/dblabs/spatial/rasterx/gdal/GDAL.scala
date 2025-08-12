package com.dblabs.spatial.rasterx.gdal

import com.databricks.labs.mosaic.utils.{HadoopUtils, PathUtils}
import com.dblabs.spatial.expressions.ExpressionConfig
import com.dblabs.spatial.rasterx.gdal.old.FormatLookup
import org.gdal.gdal.{Dataset, gdal}

object GDAL {

    /**
     * Returns the extension of the given driver.
     * @param driverShortName
     *   The short name of the driver. For example, GTiff.
     * @return
     *   Returns the extension of the driver. For example, tif.
     */
    def getExtension(driverShortName: String): String = {
        val driver = gdal.GetDriverByName(driverShortName)
        val result = driver.GetMetadataItem("DMD_EXTENSION")
        val toReturn = if (result == null) FormatLookup.formats(driverShortName) else result
        driver.delete()
        toReturn
    }

    def readRaster(
        path: String,
        readerType: RasterReader,
        config: ExpressionConfig) : Dataset = {
        val tmpPath = HadoopUtils.copyToLocalTmp(path, config.hConf)
        val readPath = PathUtils.getCleanPath(tmpPath)
    }

}
