package com.databricks.labs.mosaic.core.raster.operator

import com.databricks.labs.mosaic.core.raster.gdal.{RasterGDAL, RasterWriteOptions}
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALTranslate
import com.databricks.labs.mosaic.functions.ExprConfig
import com.databricks.labs.mosaic.gdal.MosaicGDAL
import com.databricks.labs.mosaic.utils.PathUtils

import scala.util.Try

object TranslateToGTiff {

    /**
     * Translate a RasterGDAL [[org.gdal.gdal.Dataset]] to GeoTiff.
     *
     * @param inRaster
     *   [[RasterGDAL]] to translate.
     * @param exprConfigOpt
     *   Option [[ExprConfig]]
     * @return
     *   New RasterGDAL translated to GeoTiff.
     */
    def compute(inRaster: RasterGDAL, exprConfigOpt: Option[ExprConfig]): RasterGDAL = {

        // try to hydrate the provided raster
        inRaster.getDatasetOpt() match {
            case Some(dataset) =>
                if (Try(dataset.GetSpatialRef()).isFailure || dataset.GetSpatialRef() == null) {
                    // if SRS not set, try to set it to WGS84
                    Try(dataset.SetSpatialRef(MosaicGDAL.WSG84))
                }
                val tifPath = PathUtils.createTmpFilePath("tif", exprConfigOpt)
                // Modify defaults
                // - essentially `RasterWriteOptions.GTiff`
                //   with the SRS set.
                val outOptions = RasterWriteOptions(
                    crs = dataset.GetSpatialRef() // default - MosaicGDAL.WSG84
                )

                GDALTranslate.executeTranslate(
                    tifPath,
                    inRaster,
                    command = s"""gdal_translate""",
                    outOptions,
                    exprConfigOpt
                )
            case _ =>
                val result = RasterGDAL() // <- empty raster
                result.updateLastCmd("'gdal' format -> option 'toTif'")
                result.updateError("Dataset is invalid (prior to tif convert")
                result
        }

    }

}
