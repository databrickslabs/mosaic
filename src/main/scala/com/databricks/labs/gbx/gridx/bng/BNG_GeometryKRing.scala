package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.Geometry

case class BNG_GeometryKRing(
    geom: Expression,
    resolution: Expression,
    k: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(geom, resolution, k)
    override def dataType: DataType = ArrayType(StringType)
    override def nullable: Boolean = true
    override def prettyName: String = BNG_GeometryKRing.name
    override def replacement: Expression = invoke(BNG_GeometryKRing)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

}

object BNG_GeometryKRing extends WithExpressionInfo {

    def eval(wkb: Array[Byte], resolution: Int, k: Int): Array[String] = {
        val geometry = JTS.fromWKB(wkb)
        val result = execute(geometry, resolution, k)
        result.toArray
    }

    def eval(wkt: String, resolution: Int, k: Int): Array[String] = {
        val geometry = JTS.fromWKT(wkt)
        val result = execute(geometry, resolution, k)
        result.toArray
    }

    def execute(geom: Geometry, resolution: Int, k: Int): Set[String] = {
        val kRing = BNG.geometryKRing(geom, resolution, k)
        kRing.map(BNG.format)
    }

    override def name: String = "gbx_bng_geometrykring"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_GeometryKRing(c(0), c(1), c(2))

    //TODO: ADD EXPRESSION INFO
    override def usageArgs: String = ""

    override def description: String = ""

    override def extendedUsageArgs: String = ""

    override def examples: String = ""

}
