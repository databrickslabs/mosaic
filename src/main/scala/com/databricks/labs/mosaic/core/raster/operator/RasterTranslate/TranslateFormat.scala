package com.databricks.labs.mosaic.core.raster.operator.RasterTranslate

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALTranslate
import com.databricks.labs.mosaic.utils.PathUtils

object TranslateFormat {

    /**
     * Converts the data type of a raster's bands
     *
     * @param raster
     *   The raster to update.
     * @param newFormat
     *   The new format of the raster.
     * @return
     *   A MosaicRasterGDAL object.
     */
    def update(
                  raster: MosaicRasterGDAL,
                  newFormat: String
              ): MosaicRasterGDAL = {

        val outOptions = raster.getWriteOptions.copy(format = newFormat, extension = GDAL.getExtension(newFormat))
        val resultFileName = PathUtils.createTmpFilePath(outOptions.extension)

        val result = GDALTranslate.executeTranslate(
            resultFileName,
            raster,
            command = s"gdal_translate",
            outOptions
        )

        result
    }
}
