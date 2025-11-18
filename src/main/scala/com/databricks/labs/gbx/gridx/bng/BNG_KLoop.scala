package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.BNG
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class BNG_KLoop(
    cellId: Expression,
    k: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(cellId, k)
    override def dataType: DataType = ArrayType(StringType)
    override def nullable: Boolean = true
    override def prettyName: String = BNG_KLoop.name
    override def replacement: Expression = invoke(BNG_KLoop)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

object BNG_KLoop extends WithExpressionInfo {

    def eval(cellId: UTF8String, k: Int): ArrayData = {
        val indices = execute(cellId.toString, k).map(UTF8String.fromString).toArray
        ArrayData.toArrayData(indices)
    }

    def eval(cellId: Long, k: Int): ArrayData = {
        val indices = execute(BNG.format(cellId), k).map(UTF8String.fromString).toArray
        ArrayData.toArrayData(indices)
    }

    def execute(cellId: String, k: Int): Iterator[String] = {
        BNG.kLoop(BNG.parse(cellId), k).map(BNG.format)
    }

    def execute(cellId: Long, k: Int): Iterator[String] = {
        BNG.kLoop(cellId, k).map(BNG.format)
    }

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_KLoop(c(0), c(1))

    override def name: String = "gbx_bng_kloop"

    //TODO: ADD EXPRESSION INFO
    override def usageArgs: String = ""

    override def description: String = ""

    override def extendedUsageArgs: String = ""

    override def examples: String = ""

}
