package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.gridx.grid.BNG
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class BNG_KLoop(
    cellId: Expression,
    k: Expression
) extends InvokedExpression
      with WithNewChildren {

    override def children: Seq[Expression] = Seq(cellId, k)
    override def dataType: DataType = ArrayType(cellId.dataType)
    override def nullable: Boolean = true
    override def prettyName: String = "bng_cellkloop"
    override def replacement: Expression = invoke(BNG_KLoop)

}

object BNG_KLoop extends WithExpressionInfo {

    def eval(cellId: UTF8String, k: Int): Any = {
        val indices = BNG.kLoop(BNG.parse(cellId.toString), k).map(BNG.format)
        ArrayData.toArrayData(indices)
    }

    def eval(cellId: Long, k: Int): Any = {
        val indices = BNG.kLoop(cellId, k)
        ArrayData.toArrayData(indices)
    }

    override def builder(expressionConfig: ExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[BNG_KLoop](2, expressionConfig)
    }

    override def name: String = "bng_cellkloop"

}
