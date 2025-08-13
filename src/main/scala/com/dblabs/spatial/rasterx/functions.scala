package com.dblabs.spatial.rasterx

import com.dblabs.spatial.expressions.{ExpressionConfig, RegistryDelegate}
import com.dblabs.spatial.rasterx.expressions.RST_Avg
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}

object functions extends Serializable {

    def register(spark: SparkSession): Unit = {
        val expressionConfig = ExpressionConfig(spark)
        val registry = spark.sessionState.functionRegistry
        val rd = RegistryDelegate(registry)

        rd.registerExpression[RST_Avg](expressionConfig)
    }

    def rst_avg(tileExpr: Column): Column = ColumnAdapter("rst_avg", Seq(tileExpr))

}
