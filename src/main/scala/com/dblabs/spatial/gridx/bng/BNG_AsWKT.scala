package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.gridx.grid.BNG
import com.dblabs.spatial.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class BNG_AsWKT(
    indexID: Expression
) extends InvokedExpression
      with WithNewChildren {

    override def children: Seq[Expression] = Seq(indexID)
    override def dataType: DataType = StringType
    override def nullable: Boolean = true
    override def prettyName: String = "bng_aswkb"
    override def replacement: Expression = invoke(BNG_AsWKT)

}

object BNG_AsWKT extends WithExpressionInfo {

    def eval(indexID: Long): UTF8String = {
        val geom = evalLong(indexID)
        UTF8String.fromString(geom)
    }

    def evalLong(indexID: Long): String = {
        val geom = BNG.indexToGeometry(indexID)
        JTS.toWKT(geom)
    }

    def eval(indexID: String): UTF8String = {
        val geom = evalString(indexID)
        UTF8String.fromString(geom)
    }

    def evalString(indexID: String): String = {
        val geom = BNG.indexToGeometry(BNG.parse(indexID))
        JTS.toWKT(geom)
    }

    override def name: String = "bng_aswkb"

    override def builder(expressionConfig: ExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[BNG_AsWKB](1, expressionConfig)
    }

}
