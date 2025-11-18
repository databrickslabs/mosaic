package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

case class BNG_Tessellate(
    geom: Expression,
    resolution: Expression,
    keepCoreGeom: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(geom, resolution, keepCoreGeom)
    override def dataType: DataType = ArrayType(BNG.cellType(StringType))
    override def nullable: Boolean = true
    override def prettyName: String = BNG_Tessellate.name
    override def replacement: Expression = invoke(BNG_Tessellate)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

}

object BNG_Tessellate extends WithExpressionInfo {

    def eval(wkt: UTF8String, resolution: Int, keepCoreGeom: Boolean): Array[InternalRow] = {
        val chips = executeWKT(wkt.toString, resolution, keepCoreGeom)
            .map(c => InternalRow.fromSeq(Seq(UTF8String.fromString(c._1), c._2, JTS.toWKB(c._3))))
        chips.toArray
    }

    def eval(wkb: Array[Byte], resolution: Int, keepCoreGeom: Boolean): Array[InternalRow] = {
        val chips = executeWKB(wkb, resolution, keepCoreGeom)
            .map(c => InternalRow.fromSeq(Seq(UTF8String.fromString(c._1), c._2, JTS.toWKB(c._3))))
        chips.toArray
    }

    def eval(wkt: UTF8String, resolution: UTF8String, keepCoreGeom: Boolean): Array[InternalRow] = {
        val chips = executeWKT(wkt.toString, BNG.resolutionMap(resolution.toString), keepCoreGeom)
            .map(c => InternalRow.fromSeq(Seq(UTF8String.fromString(c._1), c._2, JTS.toWKB(c._3))))
        chips.toArray
    }

    def eval(wkb: Array[Byte], resolution: UTF8String, keepCoreGeom: Boolean): Array[InternalRow] = {
        val chips = executeWKB(wkb, BNG.resolutionMap(resolution.toString), keepCoreGeom)
            .map(c => InternalRow.fromSeq(Seq(UTF8String.fromString(c._1), c._2, JTS.toWKB(c._3))))
        chips.toArray
    }

    def executeWKT(wkt: String, resolution: Int, keepCoreGeom: Boolean): Iterator[(String, Boolean, Geometry)] = {
        val geometry: Geometry = JTS.fromWKT(wkt)
        BNG.tessellate(geometry, resolution, keepCoreGeom).map(c => c.copy(_1 = BNG.format(c._1)))
    }

    def executeWKB(bytes: Array[Byte], i: Int, bool: Boolean): Iterator[(String, Boolean, Geometry)] = {
        val geometry: Geometry = JTS.fromWKB(bytes)
        BNG.tessellate(geometry, i, bool).map(c => c.copy(_1 = BNG.format(c._1)))
    }

    override def name: String = "gbx_bng_tessellate"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_Tessellate(c(0), c(1), c(2))

    //TODO: ADD EXPRESSION INFO
    override def usageArgs: String = ""

    override def description: String = ""

    override def extendedUsageArgs: String = ""

    override def examples: String = ""

}
