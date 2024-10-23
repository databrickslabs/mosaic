package com.databricks.labs.mosaic.core.raster.operator.RasterTranslate

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALTranslate
import com.databricks.labs.mosaic.utils.PathUtils

object TranslateType {

    /**
     * Converts the data type of a raster's bands
     *
     * @param raster
     *   The raster to update.
     * @param newType
     *   The new data type of the raster.
     * @return
     *   A MosaicRasterGDAL object.
     */
    def update(
                  raster: MosaicRasterGDAL,
                  newType: String
              ): MosaicRasterGDAL = {
        val outShortName = raster.getDriversShortName
        val resultFileName = PathUtils.createTmpFilePath(GDAL.getExtension(outShortName))
        val outOptions = raster.getWriteOptions

        val result = GDALTranslate.executeTranslate(
            resultFileName,
            raster,
            command = s"gdal_translate -ot $newType",
            outOptions
        )

        result
    }
}
