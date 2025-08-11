package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.gridx.grid.BNG
import com.dblabs.spatial.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

case class BNG_Tessellate(
    geom: Expression,
    resolution: Expression,
    keepCoreGeom: Expression
) extends InvokedExpression
      with WithNewChildren {

    override def children: Seq[Expression] = Seq(geom, resolution, keepCoreGeom)
    override def dataType: DataType = ArrayType(BNG.cellType(StringType))
    override def nullable: Boolean = true
    override def prettyName: String = "bng_tessellate"
    override def replacement: Expression = invoke(BNG_Tessellate)

}

object BNG_Tessellate extends WithExpressionInfo {

    def eval(wkt: UTF8String, resolution: Int, keepCoreGeom: Boolean): ArrayData = {
        val chips = evalWKT(wkt.toString, resolution, keepCoreGeom)
            .map(c => InternalRow.fromSeq(Seq(c._1, c._2, c._3)))
        ArrayData.toArrayData(chips)
    }

    def evalWKT(wkt: String, resolution: Int, keepCoreGeom: Boolean): Iterator[(Boolean, String, Geometry)] = {
        val geometry: Geometry = JTS.fromWKT(wkt)
        BNG.tessellate(geometry, resolution, keepCoreGeom).map(c => c.copy(_2 = BNG.format(c._2)))
    }

    def eval(wkb: Array[Byte], resolution: Int, keepCoreGeom: Boolean): ArrayData = {
        val chips = evalWKB(wkb, resolution, keepCoreGeom)
            .map(c => InternalRow.fromSeq(Seq(c._1, c._2, c._3)))
        ArrayData.toArrayData(chips)
    }

    def evalWKB(bytes: Array[Byte], i: Int, bool: Boolean): Iterator[(Boolean, String, Geometry)] = {
        val geometry: Geometry = JTS.fromWKB(bytes)
        BNG.tessellate(geometry, i, bool).map(c => c.copy(_2 = BNG.format(c._2)))
    }

    override def name: String = "bng_tessellate"

    override def builder(expressionConfig: ExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[BNG_Tessellate](3, expressionConfig)
    }

}
