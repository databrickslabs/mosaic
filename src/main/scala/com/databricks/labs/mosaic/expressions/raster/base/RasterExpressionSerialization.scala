package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import com.databricks.labs.mosaic.core.raster.gdal_raster.RasterCleaner
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.types.DataType

trait RasterExpressionSerialization {

    def serialize(
        data: Any,
        returnsRaster: Boolean,
        outputDataType: DataType,
        rasterAPI: RasterAPI,
        expressionConfig: MosaicExpressionConfig
    ): Any = {
        if (returnsRaster) {
            val raster = data.asInstanceOf[MosaicRaster]
            val checkpoint = expressionConfig.getRasterCheckpoint
            val result = rasterAPI.writeRasters(Seq(raster), checkpoint, outputDataType).head
            RasterCleaner.dispose(raster)
            result
        } else {
            data
        }
    }

}
