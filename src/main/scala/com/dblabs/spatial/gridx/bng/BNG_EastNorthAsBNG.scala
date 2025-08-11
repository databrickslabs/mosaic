package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.gridx.grid.BNG
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class BNG_EastNorthAsBNG(
    easting: Expression,
    northing: Expression,
    resolution: Expression
) extends InvokedExpression
      with WithNewChildren {

    override def children: Seq[Expression] = Seq(easting, northing, resolution)
    override def dataType: DataType = StringType
    override def nullable: Boolean = true
    override def prettyName: String = "bng_eastnorthasbng"
    override def replacement: Expression = invoke(BNG_EastNorthAsBNG)

}

object BNG_EastNorthAsBNG extends WithExpressionInfo {

    def eval(easting: Double, northing: Double, resolution: Any): UTF8String = {
        val cellId = resolution match {
            case r: Int        => evalInt(easting, northing, r)
            case r: UTF8String => evalString(easting, northing, r.toString)
        }
        UTF8String.fromString(cellId)
    }

    def evalString(easting: Double, northing: Double, resolution: String): String = {
        val res = BNG.resolutionMap(resolution)
        val cellID = BNG.pointToIndex(easting, northing, res)
        BNG.format(cellID)
    }

    def evalInt(easting: Double, northing: Double, resolution: Int): String = {
        val cellID = BNG.pointToIndex(easting, northing, resolution)
        BNG.format(cellID)
    }

    override def name: String = "bng_eastnorthasbng"

    override def builder(expressionConfig: ExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[BNG_EastNorthAsBNG](3, expressionConfig)
    }

}
