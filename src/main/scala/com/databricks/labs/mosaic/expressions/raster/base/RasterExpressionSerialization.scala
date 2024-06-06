package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.index.IndexSystemFactory
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.types.DataType

/**
  * Base trait for raster serialization. It is used to serialize the result of
  * the expression.
  */
trait RasterExpressionSerialization extends RasterPathAware {

    /**
      * Serializes the result of the expression. If the expression returns a
      * raster, the raster is serialized. If the expression returns a scalar,
      * the scalar is returned.
      * @param data
      *   The result of the expression.
      * @param returnsRaster
      *   Whether the expression returns a raster.
      * @param outputDataType
      *   The output data type of the expression.
      * @param expressionConfig
      *   Additional arguments for the expression (expressionConfigs).
      * @return
      *   The serialized result of the expression.
      */
    def serialize(
        data: Any,
        returnsRaster: Boolean,
        outputDataType: DataType,
        expressionConfig: MosaicExpressionConfig
    ): Any = {
        if (returnsRaster) {
            val manualMode = expressionConfig.isManualCleanupMode
            val tile = data.asInstanceOf[MosaicRasterTile]
            val result = tile.formatCellId(IndexSystemFactory.getIndexSystem(expressionConfig.getIndexSystem))
            val serialized = result.serialize(outputDataType, doDestroy = true, manualMode)
            pathSafeDispose(result, manualMode)
            serialized
        } else {
            data
        }
    }

}
