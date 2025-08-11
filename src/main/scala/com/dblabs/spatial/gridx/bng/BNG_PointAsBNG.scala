package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.gridx.grid.BNG
import com.dblabs.spatial.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class BNG_PointAsBNG(
    geom: Expression,
    resolution: Expression
) extends InvokedExpression
      with WithNewChildren {

    override def children: Seq[Expression] = Seq(geom, resolution)
    override def dataType: DataType = StringType
    override def nullable: Boolean = true
    override def prettyName: String = "bng_pointasbng"
    override def replacement: Expression = invoke(BNG_PointAsBNG)

}

object BNG_PointAsBNG extends WithExpressionInfo {

    def eval(wkt: UTF8String, resolution: Int): UTF8String = {
        val cellID = evalWKT(wkt.toString, resolution)
        UTF8String.fromString(cellID)
    }

    def evalWKT(wkt: String, resolution: Int): String = {
        val geometry = JTS.fromWKT(wkt)
        val cellID = BNG.pointToIndex(geometry.getCentroid.getX, geometry.getCentroid.getY, resolution)
        BNG.format(cellID)
    }

    def eval(wkb: Array[Byte], resolution: Int): UTF8String = {
        val cellID = evalWKB(wkb, resolution)
        UTF8String.fromString(cellID)
    }

    def evalWKB(bytes: Array[Byte], resolution: Int): String = {
        val geometry = JTS.fromWKB(bytes)
        val cellID = BNG.pointToIndex(geometry.getCentroid.getX, geometry.getCentroid.getY, resolution)
        BNG.format(cellID)
    }

    override def name: String = "bng_pointasbng"

    override def builder(expressionConfig: ExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[BNG_PointAsBNG](2, expressionConfig)
    }

}
