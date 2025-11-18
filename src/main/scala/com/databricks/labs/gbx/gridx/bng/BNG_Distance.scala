package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.BNG
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class BNG_Distance(
    cellId: Expression,
    cellId2: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(cellId, cellId2)
    override def dataType: DataType = LongType
    override def nullable: Boolean = true
    override def prettyName: String = BNG_Distance.name
    override def replacement: Expression = invoke(BNG_Distance)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

object BNG_Distance extends WithExpressionInfo {

    def eval(cellId: Long, cellId2: Long): Long = execute(cellId, cellId2)
    def eval(cellId: UTF8String, cellId2: UTF8String): Long = execute(cellId.toString, cellId2.toString)

    def execute(cellId: Long, cellId2: Long): Long = BNG.distance(cellId, cellId2)

    def execute(cellId: String, cellId2: String): Long = {
        val cellIdLong = BNG.parse(cellId)
        val cellId2Long = BNG.parse(cellId2)
        BNG.distance(cellIdLong, cellId2Long)
    }

    override def name: String = "gbx_bng_distance"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_Distance(c(0), c(1))

    //TODO: ADD EXPRESSION INFO
    override def usageArgs: String = ""

    override def description: String = ""

    override def extendedUsageArgs: String = ""

    override def examples: String = ""

}
