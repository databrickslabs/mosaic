package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

case class BNG_Polyfill(
    geom: Expression,
    resolution: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(geom, resolution)
    override def dataType: DataType = ArrayType(StringType)
    override def nullable: Boolean = true
    override def prettyName: String = BNG_Polyfill.name
    override def replacement: Expression = invoke(BNG_Polyfill)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

object BNG_Polyfill extends WithExpressionInfo {

    def eval(geom: UTF8String, resolution: UTF8String): Array[String] = {
        val geometry = JTS.fromWKT(geom.toString)
        val cells = execute(geometry, resolution.toString)
        cells.toArray
    }

    def eval(geom: UTF8String, resolution: Int): Array[String] = {
        val geometry = JTS.fromWKT(geom.toString)
        val cells = execute(geometry, resolution)
        cells.toArray
    }

    def eval(geom: Array[Byte], resolution: UTF8String): Array[String] = {
        val geometry = JTS.fromWKB(geom)
        val cells = execute(geometry, resolution.toString)
        cells.toArray
    }

    def eval(geom: Array[Byte], resolution: Int): Array[String] = {
        val geometry = JTS.fromWKB(geom)
        val cells = execute(geometry, resolution)
        cells.toArray
    }

    def execute(geom: Geometry, resolution: Int): Iterator[String] = {
        BNG.polyfill(geom, resolution)
            .map(BNG.format)
    }

    def execute(geom: Geometry, resolution: String): Iterator[String] = {
        val res = BNG.resolutionMap(resolution)
        BNG.polyfill(geom, res)
            .map(BNG.format)
    }

    override def name: String = "gbx_bng_polyfill"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_Polyfill(c(0), c(1))

    //TODO: ADD EXPRESSION INFO
    override def usageArgs: String = ""

    override def description: String = ""

    override def extendedUsageArgs: String = ""

    override def examples: String = ""

}
