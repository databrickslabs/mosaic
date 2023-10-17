package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.WithExpressionInfo
import com.databricks.labs.mosaic.expressions.geometry.base.UnaryVector2ArgExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.adapters.Column
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

/**
  * SQL expression that returns the input geometry buffered by the radius.
  * @param inputGeom
  *   Expression containing the geometry.
  * @param radiusExpr
  *   The radius of the buffer.
  * @param expressionConfig
  *   Mosaic execution context, e.g. geometryAPI, indexSystem, etc. Additional
  *   arguments for the expression (expressionConfigs).
  */
case class ST_BufferCapStyle(
    inputGeom: Expression,
    radiusExpr: Expression,
    capStyleExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends UnaryVector2ArgExpression[ST_BufferCapStyle](inputGeom, radiusExpr, capStyleExpr, returnsGeometry = true, expressionConfig) {

    override def dataType: DataType = inputGeom.dataType

    override def geometryTransform(geometry: MosaicGeometry, arg1: Any, arg2: Any): Any = {
        val radius = arg1.asInstanceOf[Double]
        val capStyle = arg2.asInstanceOf[UTF8String].toString
        geometry.bufferCapStyle(radius, capStyle)
    }

    override def geometryCodeGen(geometryRef: String, argRef1: String, argRef2: String, ctx: CodegenContext): (String, String) = {
        val resultRef = ctx.freshName("result")
        val code = s"""$mosaicGeomClass $resultRef = $geometryRef.bufferCapStyle($argRef1, $argRef2);"""
        (code, resultRef)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_BufferCapStyle extends WithExpressionInfo {

    override def name: String = "st_buffer_cap_style"

    override def usage: String = "_FUNC_(expr1, expr2) - Returns buffered geometry."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, b);
          |        POLYGON (...)
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = { (children: Seq[Expression]) =>
        ST_BufferCapStyle(children.head, Column(children(1)).cast("double").expr, children(2), expressionConfig)
    }

}
