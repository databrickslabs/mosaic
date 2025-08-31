package com.dblabs.spatial.rasterx.operations

import com.dblabs.spatial.rasterx.gdal.GDAL
import com.dblabs.spatial.rasterx.operator.GDALTranslate
import org.gdal.gdal.Dataset

object TranslateFormat {

    /**
      * Converts the data type of a raster's bands
      *
      * @param raster
      *   The raster to update.
      * @param newFormat
      *   The new format of the raster.
      * @return
      *   A Raster object.
      */
    def update(
        raster: Dataset,
        options: Map[String, String],
        newFormat: String
    ): (Dataset, Map[String, String]) = {

        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val extension = GDAL.getExtension(newFormat)
        val resultFileName = s"/vsimem/translate_format_$uuid.$extension"

        val result = GDALTranslate.executeTranslate(
          resultFileName,
          raster,
          command = s"gdal_translate",
          options ++ Map(
            "format" -> newFormat,
            "extension" -> extension
          )
        )

        result
    }
}
