package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.gridx.grid.BNG
import com.dblabs.spatial.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.Geometry

case class BNG_GeometryKRing(
    geom: Expression,
    resolution: Expression,
    k: Expression
) extends InvokedExpression
      with WithNewChildren {

    override def children: Seq[Expression] = Seq(geom, resolution, k)
    override def dataType: DataType = ArrayType(StringType)
    override def nullable: Boolean = true
    override def prettyName: String = "bng_geometrykring"
    override def replacement: Expression = invoke(BNG_KRing)

}

object BNG_GeometryKRing extends WithExpressionInfo {

    def eval(wkb: Array[Byte], resolution: Int, k: Int): Any = {
        val geometry = JTS.fromWKB(wkb)
        val kRing = BNG.geometryKRing(geometry, resolution, k)
        val formatted = kRing.map(BNG.format)
        ArrayData.toArrayData(formatted.toArray)
    }

    def eval(wkt: String, resolution: Int, k: Int): Any = {
        val geometry = JTS.fromWKT(wkt)
        val kRing = BNG.geometryKRing(geometry, resolution, k)
        val formatted = kRing.map(BNG.format)
        ArrayData.toArrayData(formatted.toArray)
    }

    def eval(geom: Geometry, resolution: Int, k: Int): Any = BNG.geometryKRing(geom, resolution, k)

    override def name: String = "bng_geometrykring"

    override def builder(expressionConfig: ExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[BNG_GeometryKRing](3, expressionConfig)
    }

}
