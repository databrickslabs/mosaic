package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.geometry.base.UnaryVector2ArgExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.DataType

/**
  * SQL expression that returns the buffer loop of the input geometry.
  * @param inputGeom
  *   Expression containing the geometry.
  * @param innerRadius
  *   Expression containing the inner radius.
  * @param outerRadius
  *   Expression containing the outer radius.
  * @param expressionConfig
  *   Mosaic execution context, e.g. geometryAPI, indexSystem, etc. Additional
  *   arguments for the expression (expressionConfigs).
  */
case class ST_BufferLoop(
    inputGeom: Expression,
    innerRadius: Expression,
    outerRadius: Expression,
    expressionConfig: MosaicExpressionConfig
) extends UnaryVector2ArgExpression[ST_BufferLoop](
      inputGeom,
      innerRadius,
      outerRadius,
      returnsGeometry = true,
      expressionConfig
    ) {

    override def dataType: DataType = inputGeom.dataType

    override def geometryTransform(geometry: MosaicGeometry, arg1: Any, arg2: Any): Any = {
        val innerRadius = arg1.asInstanceOf[Double]
        val outerRadius = arg2.asInstanceOf[Double]
        geometry.buffer(outerRadius).difference(geometry.buffer(innerRadius))
    }

    override def geometryCodeGen(geometryRef: String, arg1Ref: String, arg2Ref: String, ctx: CodegenContext): (String, String) = {
        val resultRef = ctx.freshName("result")
        val code = s"""$mosaicGeomClass $resultRef = $geometryRef.buffer($arg2Ref).difference($geometryRef.buffer($arg1Ref));"""
        (code, resultRef)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_BufferLoop extends WithExpressionInfo {

    override def name: String = "st_bufferloop"

    override def usage: String = "_FUNC_(expr1, expr2, expr3) - Returns the buffer loop of the geometry."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, r1, r2);
          |        POLYGON(...) / MULTIPOLYGON(...)
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_BufferLoop](3, expressionConfig)
    }

}
