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
import org.apache.spark.sql.functions._

/**
  * SQL expression that returns the input geometry buffered by the radius.
  * @param inputGeom
  *   Expression containing the geometry.
  * @param radiusExpr
  *   The radius of the buffer.
  * @param bufferStyleParametersExpr
  *   'quad_segs=# endcap=round|flat|square' where "#" is the number of line
  *   segments used to approximate a quarter circle (default is 8); and endcap
  *   style for line features is one of listed (default="round")
  * @param expressionConfig
  *   Mosaic execution context, e.g. geometryAPI, indexSystem, etc. Additional
  *   arguments for the expression (expressionConfigs).
  */
case class ST_Buffer(
    inputGeom: Expression,
    radiusExpr: Expression,
    bufferStyleParametersExpr: Expression = lit("").expr,
    expressionConfig: MosaicExpressionConfig
) extends UnaryVector2ArgExpression[ST_Buffer](inputGeom, radiusExpr, bufferStyleParametersExpr, returnsGeometry = true, expressionConfig) {

    override def dataType: DataType = inputGeom.dataType

    override def geometryTransform(geometry: MosaicGeometry, arg1: Any, arg2: Any): Any = {
        val radius = arg1.asInstanceOf[Double]
        val bufferStyleParameters = arg2.asInstanceOf[UTF8String].toString
        geometry.buffer(radius, bufferStyleParameters)
    }

    override def geometryCodeGen(geometryRef: String, argRef1: String, argRef2: String, ctx: CodegenContext): (String, String) = {
        val resultRef = ctx.freshName("result")
        val code = s"""$mosaicGeomClass $resultRef = $geometryRef.buffer($argRef1, $argRef2.toString());"""
        (code, resultRef)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_Buffer extends WithExpressionInfo {

    override def name: String = "st_buffer"

    override def usage: String = "_FUNC_(expr1, expr2) - Returns buffered geometry."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, b);
          |        POLYGON (...)
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = { (children: Seq[Expression]) =>
        if (children.size == 2) {
            ST_Buffer(children.head, Column(children(1)).cast("double").expr, lit("").expr, expressionConfig)
        } else if (children.size == 3) {
            ST_Buffer(children.head, Column(children(1)).cast("double").expr, Column(children(2)).cast("string").expr, expressionConfig)
        } else throw new Exception("unexpected number of arguments")
    }

}
