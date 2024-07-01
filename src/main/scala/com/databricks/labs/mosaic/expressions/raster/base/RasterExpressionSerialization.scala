package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.index.IndexSystemFactory
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.types.DataType

/**
  * Base trait for raster serialization. It is used to serialize the result of
  * the expression.
  */
trait RasterExpressionSerialization {

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
      * @param exprConfig
      *   Additional arguments for the expression (expressionConfigs).
      * @return
      *   The serialized result of the expression.
      */
    def serialize(
                     data: Any,
                     returnsRaster: Boolean,
                     outputDataType: DataType,
                     doDestroy: Boolean,
                     exprConfig: ExprConfig
    ): Any = {
        if (returnsRaster) {
            val tile = data.asInstanceOf[RasterTile]
            val result = tile.formatCellId(IndexSystemFactory.getIndexSystem(exprConfig.getIndexSystem))
            val serialized = result.serialize(outputDataType, doDestroy, Option(exprConfig))

            serialized
        } else {
            data
        }
    }

}
