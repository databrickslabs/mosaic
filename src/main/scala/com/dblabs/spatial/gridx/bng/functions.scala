package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.expressions.{ExpressionConfig, RegistryDelegate}
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.{Column, SparkSession}

object functions extends Serializable {

    def register(spark: SparkSession): Unit = {
        val expressionConfig = ExpressionConfig(spark)
        val registry = spark.sessionState.functionRegistry
        val rd = RegistryDelegate(registry)

        rd.registerExpression[BNGCellArea](expressionConfig)
        rd.registerExpression[BNGCellIntersection](expressionConfig)
    }

    def bng_cell_area(cellId: Column): Column = ColumnAdapter(BNGCellArea(cellId.expr))

    def bng_cell_intersection(leftChip: Column, rightChip: Column): Column = {
        ColumnAdapter(BNGCellIntersection(leftChip.expr, rightChip.expr))
    }

}
