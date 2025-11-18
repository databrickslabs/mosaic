package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class BNG_PointAsBNG(
    geom: Expression,
    resolution: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(geom, resolution)
    override def dataType: DataType = StringType
    override def nullable: Boolean = true
    override def prettyName: String = BNG_PointAsBNG.name
    override def replacement: Expression = invoke(BNG_PointAsBNG)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

object BNG_PointAsBNG extends WithExpressionInfo {

    def eval(wkt: UTF8String, resolution: Int): UTF8String = {
        val cellID = executeWKT(wkt.toString, resolution)
        UTF8String.fromString(cellID)
    }

    def eval(wkb: Array[Byte], resolution: Int): UTF8String = {
        val cellID = executeWKB(wkb, resolution)
        UTF8String.fromString(cellID)
    }

    def executeWKT(wkt: String, resolution: Int): String = {
        val geometry = JTS.fromWKT(wkt)
        val cellID = BNG.pointToCellID(geometry.getCentroid.getX, geometry.getCentroid.getY, resolution)
        BNG.format(cellID)
    }

    def executeWKB(bytes: Array[Byte], resolution: Int): String = {
        val geometry = JTS.fromWKB(bytes)
        val cellID = BNG.pointToCellID(geometry.getCentroid.getX, geometry.getCentroid.getY, resolution)
        BNG.format(cellID)
    }

    override def name: String = "gbx_bng_pointasbng"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_PointAsBNG(c(0), c(1))

    //TODO: ADD EXPRESSION INFO
    override def usageArgs: String = ""

    override def description: String = ""

    override def extendedUsageArgs: String = ""

    override def examples: String = ""

}
