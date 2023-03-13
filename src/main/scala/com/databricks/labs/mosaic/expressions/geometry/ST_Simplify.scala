package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.geometry.base.UnaryVector1ArgExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.DataType

/**
  * SQL expression that returns the input geometry simplified respecting the
  * tolerance.
  * @param inputGeom
  *   Expression containing the geometry.
  * @param toleranceExpr
  *   The tolerance of the simplification.
  * @param expressionConfig
  *   Mosaic execution context, e.g. geometryAPI, indexSystem, etc. Additional
  *   arguments for the expression (expressionConfigs).
  */
case class ST_Simplify(
    inputGeom: Expression,
    toleranceExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends UnaryVector1ArgExpression[ST_Simplify](
      inputGeom,
      toleranceExpr,
      returnsGeometry = true,
      expressionConfig
    ) {

    override def dataType: DataType = inputGeom.dataType

    override def geometryTransform(geometry: MosaicGeometry, arg: Any): Any = {
        geometry.simplify(arg.asInstanceOf[Double])
    }

    override def geometryCodeGen(geometryRef: String, argRef: String, ctx: CodegenContext): (String, String) = {
        val resultRef = ctx.freshName("result")
        val code = s"""$mosaicGeomClass $resultRef = $geometryRef.simplify($argRef);"""
        (code, resultRef)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_Simplify extends WithExpressionInfo {

    override def name: String = "st_simplify"

    override def usage: String = "_FUNC_(expr1, expr2) - Returns simplified geometry."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, b);
          |        POLYGON (...)
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_Simplify](2, expressionConfig)
    }

}
