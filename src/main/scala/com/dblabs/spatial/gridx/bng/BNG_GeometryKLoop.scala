package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.gridx.grid.BNG
import com.dblabs.spatial.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class BNG_GeometryKLoop(
    geom: Expression,
    resolution: Expression,
    k: Expression
) extends InvokedExpression
      with WithNewChildren {

    override def children: Seq[Expression] = Seq(geom, resolution, k)
    override def dataType: DataType = ArrayType(StringType)
    override def nullable: Boolean = true
    override def prettyName: String = "bng_geometrykloop"
    override def replacement: Expression = invoke(BNG_GeometryKLoop)

}

object BNG_GeometryKLoop extends WithExpressionInfo {

    def eval(geom: UTF8String, res: Int, k: Int): Any = {
        val geometry = JTS.fromWKT(geom.toString)
        val kLoop = BNG.geometryKLoop(geometry, res, k)
        val formatted = kLoop.map(BNG.format)
        ArrayData.toArrayData(formatted.toArray)
    }

    def eval(geom: Array[Byte], res: Int, k: Int): Any = {
        val geometry = JTS.fromWKB(geom)
        val kLoop = BNG.geometryKLoop(geometry, res, k)
        val formatted = kLoop.map(BNG.format)
        ArrayData.toArrayData(formatted.toArray)
    }

    override def name: String = "bng_geometrykloop"

    override def builder(expressionConfig: ExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[BNG_GeometryKLoop](3, expressionConfig)
    }

}
