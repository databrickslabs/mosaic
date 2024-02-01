package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.ArrayType

object RasterArrayUtils {

    def getTiles(input: Any, rastersExpr: Expression, expressionConfig: MosaicExpressionConfig): Seq[MosaicRasterTile] = {
        val rasterDT = rastersExpr.dataType.asInstanceOf[ArrayType].elementType
        val arrayData = input.asInstanceOf[ArrayData]
        val rasterType = RasterTileType(rastersExpr).rasterType
        val n = arrayData.numElements()
        (0 until n)
            .map(i =>
                MosaicRasterTile
                    .deserialize(
                        arrayData.get(i, rasterDT).asInstanceOf[InternalRow],
                        expressionConfig.getCellIdType,
                        rasterType
                    )
            )
    }

}
