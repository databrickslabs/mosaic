package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.ArrayType

object RasterArrayUtils {

    def getTiles(input: Any, rastersExpr: Expression, exprConfig: ExprConfig): Seq[RasterTile] = {
        val rasterDT = rastersExpr.dataType.asInstanceOf[ArrayType].elementType
        val arrayData = input.asInstanceOf[ArrayData]
        val n = arrayData.numElements()
        (0 until n)
            .map(i =>
                RasterTile
                    .deserialize(
                        arrayData.get(i, rasterDT).asInstanceOf[InternalRow],
                        exprConfig.getCellIdType,
                        Option(exprConfig) // 0.4.3 infer type
                    )
            )
    }

}
