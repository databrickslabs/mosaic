package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.expressions.{ExpressionConfig, RegistryDelegate}
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.{Column, SparkSession}

object functions extends Serializable {

    def register(spark: SparkSession): Unit = {
        val expressionConfig = ExpressionConfig(spark)
        val registry = spark.sessionState.functionRegistry
        val rd = RegistryDelegate(registry)

        rd.registerExpression[BNG_CellArea](expressionConfig)
        rd.registerExpression[BNG_CellIntersection](expressionConfig)
        rd.registerExpression[BNG_KLoop](expressionConfig)
        rd.registerExpression[BNG_KLoopExplode](expressionConfig)
    }

    def bng_cell_area(cellId: Column): Column = ColumnAdapter("bng_cellarea", Seq(cellId))
    def bng_cell_intersection(leftChip: Column, rightChip: Column): Column = {
        ColumnAdapter("bng_cellintersection", Seq(leftChip, rightChip))
    }
    def bng_kloop(cellId: Column, k: Column): Column = ColumnAdapter("bng_kloop", Seq(cellId, k))
    def bng_kloopexplode(cellId: Column, k: Column): Column = ColumnAdapter("bng_kloopexplode", Seq(cellId, k))

}
