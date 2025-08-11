package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.gridx.grid.BNG
import com.dblabs.spatial.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

case class BNG_Polyfill(geom: Expression, resolution: Expression) extends InvokedExpression with WithNewChildren {

    override def children: Seq[Expression] = Seq(geom, resolution)
    override def dataType: DataType = ArrayType(StringType)
    override def nullable: Boolean = true
    override def prettyName: String = "bng_polyfill"
    override def replacement: Expression = invoke(BNG_Polyfill)

}

object BNG_Polyfill extends WithExpressionInfo {

    def eval(geom: Any, resolution: Any): ArrayData = {
        val geometry = geom match {
            case g: Array[Byte] => JTS.fromWKB(g)
            case g: String      => JTS.fromWKT(g)
        }
        val cells = (resolution match {
            case r: Int    => execute(geometry, r)
            case r: String => execute(geometry, r)
        }).map(UTF8String.fromString)
        ArrayData.toArrayData(cells.toArray)
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

    override def name: String = "bng_polyfill"

    override def builder(expressionConfig: ExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[BNG_Polyfill](2, expressionConfig)
    }

}
