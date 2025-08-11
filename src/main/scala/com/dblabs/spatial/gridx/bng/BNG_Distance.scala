package com.dblabs.spatial.gridx.bng

import com.databricks.labs.mosaic.BNG
import com.dblabs.spatial.expressions._
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._

case class BNG_Distance(
    cellId: Expression,
    cellId2: Expression
) extends InvokedExpression
      with WithNewChildren {

    override def children: Seq[Expression] = Seq(cellId, cellId2)
    override def dataType: DataType = LongType
    override def nullable: Boolean = true
    override def prettyName: String = "bng_kring"
    override def replacement: Expression = invoke(BNG_Distance)

}

object BNG_Distance extends WithExpressionInfo {

    def eval(cellId: Long, cellId2: Long): Long = {
        BNG.distance(cellId, cellId2)
    }

    def eval(cellId: String, cellId2: String): Long = {
        BNG.distance(BNG.parse(cellId), BNG.parse(cellId2))
    }

    override def name: String = "bng_distance"

    override def builder(expressionConfig: ExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[BNG_Distance](2, expressionConfig)
    }

}
