package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.BNG
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class BNG_EastNorthAsBNG(
    easting: Expression,
    northing: Expression,
    resolution: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(easting, northing, resolution)
    override def dataType: DataType = StringType
    override def nullable: Boolean = true
    override def prettyName: String = BNG_EastNorthAsBNG.name
    override def replacement: Expression = invoke(BNG_EastNorthAsBNG)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

}

object BNG_EastNorthAsBNG extends WithExpressionInfo {

    def eval(easting: Double, northing: Double, resolution: Int): UTF8String = {
        val cellId = executeInt(easting, northing, resolution)
        UTF8String.fromString(cellId)
    }

    def eval(easting: Double, northing: Double, resolution: UTF8String): UTF8String = {
        val cellId = executeString(easting, northing, resolution.toString)
        UTF8String.fromString(cellId)
    }

    def executeString(easting: Double, northing: Double, resolution: String): String = {
        val res = BNG.resolutionMap(resolution)
        val cellID = BNG.pointToCellID(easting, northing, res)
        BNG.format(cellID)
    }

    def executeInt(easting: Double, northing: Double, resolution: Int): String = {
        val cellID = BNG.pointToCellID(easting, northing, resolution)
        BNG.format(cellID)
    }

    override def name: String = "gbx_bng_eastnorthasbng"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_EastNorthAsBNG(c(0), c(1), c(2))

    //TODO: ADD EXPRESSION INFO
    override def usageArgs: String = ""

    override def description: String = ""

    override def extendedUsageArgs: String = ""

    override def examples: String = ""

}
