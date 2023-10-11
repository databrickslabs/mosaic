package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import com.databricks.labs.mosaic.core.raster.gdal_raster.RasterCleaner
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
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
            val tile = data.asInstanceOf[MosaicRasterTile]
            val checkpoint = expressionConfig.getRasterCheckpoint
            val rasterType = outputDataType.asInstanceOf[RasterTileType].rasterType
            val result = tile.serialize(rasterAPI, rasterType, checkpoint)
            RasterCleaner.dispose(tile)
            result
        } else {
            data
        }
    }

}
