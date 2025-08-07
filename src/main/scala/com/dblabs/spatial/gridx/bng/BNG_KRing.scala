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
) extends InvokedExpression
      with WithNewChildren {

    override def children: Seq[Expression] = Seq(cellId, k)
    override def dataType: DataType = ArrayType(cellId.dataType)
    override def nullable: Boolean = true
    override def prettyName: String = "bng_kring"
    override def replacement: Expression = invoke(BNG_KRing)

}

object BNG_KRing extends WithExpressionInfo {

    def eval(cellId: UTF8String, k: Int): Any = {
        val indices = BNG.kRing(BNG.parse(cellId.toString), k).map(BNG.format)
        ArrayData.toArrayData(indices)
    }

    def eval(cellId: Long, k: Int): Any = {
        val indices = BNG.kRing(cellId, k)
        ArrayData.toArrayData(indices)
    }

    override def name: String = "bng_kring"

    override def builder(expressionConfig: ExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[BNG_KRing](2, expressionConfig)
    }

}
