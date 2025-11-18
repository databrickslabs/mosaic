package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.BNG
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, DoubleType}
import org.apache.spark.unsafe.types.UTF8String

case class BNG_CellArea(
    cellIdExpression: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(cellIdExpression)
    override def dataType: DataType = DoubleType
    override def nullable: Boolean = true
    override def prettyName: String = BNG_CellArea.name
    override def replacement: Expression = invoke(BNG_CellArea)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

object BNG_CellArea extends WithExpressionInfo {

    def eval(cellId: UTF8String): Double = execute(cellId.toString)
    def eval(cellID: Long): Double = execute(cellID)

    def execute(cellID: Long): Double = BNG.area(cellID)

    def execute(cellID: String): Double = {
        val cellIdLong = BNG.parse(cellID)
        BNG.area(cellIdLong)
    }

    override def name: String = "gbx_bng_cellarea"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_CellArea(c(0))

    //TODO: ADD EXPRESSION INFO
    override def usageArgs: String = ""

    override def description: String = ""

    override def extendedUsageArgs: String = ""

    override def examples: String = ""

}
