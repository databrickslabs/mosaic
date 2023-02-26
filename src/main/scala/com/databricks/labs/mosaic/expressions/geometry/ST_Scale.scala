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
  * SQL Expression for returning the scaled geometry by.
  * @param inputGeom
  *   The input geometry expression.
  * @param xd
  *   The x distance to scale the geometry.
  * @param yd
  *   The y distance to scale the geometry.
  * @param expressionConfig
  *   Mosaic execution context, e.g. the geometry API, index system, etc.
  *   Additional arguments for the expression (expressionConfigs).
  */
case class ST_Scale(
    inputGeom: Expression,
    xd: Expression,
    yd: Expression,
    expressionConfig: MosaicExpressionConfig
) extends UnaryVector2ArgExpression[ST_Scale](
      inputGeom,
      xd,
      yd,
      returnsGeometry = true,
      expressionConfig
    ) {

    override def dataType: DataType = inputGeom.dataType

    override def geometryTransform(geometry: MosaicGeometry, arg1: Any, arg2: Any): Any = {
        val xDist = arg1.asInstanceOf[Double]
        val yDist = arg2.asInstanceOf[Double]
        geometry.scale(xDist, yDist)
    }

    override def geometryCodeGen(geometryRef: String, arg1Ref: String, arg2Ref: String, ctx: CodegenContext): (String, String) = {
        val resultRef = ctx.freshName("result")
        val code = s"""$mosaicGeomClass $resultRef = $geometryRef.scale($arg1Ref, $arg2Ref);"""
        (code, resultRef)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_Scale extends WithExpressionInfo {

    override def name: String = "st_scale"

    override def usage: String = "_FUNC_(expr1, xd, yd) - Returns a new geometry scaled using xd for x axis and yd for y axis."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, xd, yd);
          |        POLYGON ((...))
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_Scale](3, expressionConfig)
    }

}
