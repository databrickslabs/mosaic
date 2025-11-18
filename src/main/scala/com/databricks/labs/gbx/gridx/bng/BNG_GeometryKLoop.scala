package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

case class BNG_GeometryKLoop(
    geom: Expression,
    resolution: Expression,
    k: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(geom, resolution, k)
    override def dataType: DataType = ArrayType(StringType)
    override def nullable: Boolean = true
    override def prettyName: String = BNG_GeometryKLoop.name
    override def replacement: Expression = invoke(BNG_GeometryKLoop)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

}

object BNG_GeometryKLoop extends WithExpressionInfo {

    def eval(geom: UTF8String, res: Int, k: Int): Array[String] = {
        val geometry = JTS.fromWKT(geom.toString)
        val result = execute(geometry, res, k)
        result.toArray
    }

    def eval(geom: Array[Byte], res: Int, k: Int): Array[String] = {
        val geometry = JTS.fromWKB(geom)
        val result = execute(geometry, res, k)
        result.toArray
    }

    def execute(geom: Geometry, res: Int, k: Int): Set[String] = {
        val kLoop = BNG.geometryKLoop(geom, res, k)
        kLoop.map(BNG.format)
    }

    override def name: String = "gbx_bng_geometrykloop"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_GeometryKLoop(c(0), c(1), c(2))

    //TODO: ADD EXPRESSION INFO
    override def usageArgs: String = ""

    override def description: String = ""

    override def extendedUsageArgs: String = ""

    override def examples: String = ""

}
