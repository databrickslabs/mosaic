package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.gridx.grid.BNG
import com.dblabs.spatial.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

case class BNG_AsWKB(
    indexID: Expression
) extends InvokedExpression
      with WithNewChildren {

    override def children: Seq[Expression] = Seq(indexID)
    override def dataType: DataType = BinaryType
    override def nullable: Boolean = true
    override def prettyName: String = "bng_aswkb"
    override def replacement: Expression = invoke(BNG_AsWKB)

}

object BNG_AsWKB extends WithExpressionInfo {

    def eval(indexID: Long): Array[Byte] = {
        val geom = BNG.indexToGeometry(indexID)
        JTS.toWKB(geom)
    }

    def eval(indexID: String): Array[Byte] = {
        val geom = BNG.indexToGeometry(BNG.parse(indexID))
        JTS.toWKB(geom)
    }

    override def name: String = "bng_aswkb"

    override def builder(expressionConfig: ExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[BNG_AsWKB](1, expressionConfig)
    }

}
