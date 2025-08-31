package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.gridx.grid.BNG
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class BNG_KRing(
    cellId: Expression,
    k: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(cellId, k)
    override def dataType: DataType = ArrayType(StringType)
    override def nullable: Boolean = true
    override def prettyName: String = "bng_kring"
    override def replacement: Expression = invoke(BNG_KRing)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

object BNG_KRing extends WithExpressionInfo {

    def eval(cellId: UTF8String, k: Int): ArrayData = {
        val indices = execute(cellId.toString, k)
        ArrayData.toArrayData(indices)
    }

    def eval(cellId: Long, k: Int): ArrayData = {
        val indices = execute(BNG.format(cellId), k)
        ArrayData.toArrayData(indices)
    }

    def execute(cellId: String, k: Int): Iterator[String] = {
        BNG.kRing(BNG.parse(cellId), k).map(BNG.format)
    }

    override def name: String = "bng_kring"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_KRing(c(0), c(1))

}
