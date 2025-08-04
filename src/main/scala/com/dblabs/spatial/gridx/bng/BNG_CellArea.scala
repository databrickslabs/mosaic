package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.expressions.{ExpressionConfig, GenericExpressionFactory, InvokedExpression, WithExpressionInfo, WithNewChildren}
import com.dblabs.spatial.gridx.grid.BNG
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, DoubleType}
import org.apache.spark.unsafe.types.UTF8String

case class BNG_CellArea(
    cellIdExpression: Expression
) extends InvokedExpression
      with WithNewChildren {

    override def children: Seq[Expression] = Seq(cellIdExpression)
    override def dataType: DataType = DoubleType
    override def nullable: Boolean = true
    override def prettyName: String = "bng_cellarea"
    override def replacement: Expression = invoke(BNG_CellArea)

}

object BNG_CellArea extends WithExpressionInfo {

    def eval(
        cellId: UTF8String
    ): Double = {
        val cellIdLong = BNG.parse(cellId.toString)
        BNG.area(cellIdLong)
    }

    def eval(
        cellId: Long
    ): Double = {
        BNG.area(cellId)
    }

    override def name: String = "bng_cellarea"

    override def builder(expressionConfig: ExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[BNG_CellArea](1, expressionConfig)
    }

}
