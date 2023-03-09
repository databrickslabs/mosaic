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
  * SQL expression that returns the input geometry rotated by an angle.
  * @param inputGeom
  *   Expression containing the geometry.
  * @param thetaExpr
  *   The angle of rotation.
  * @param expressionConfig
  *   Mosaic execution context, e.g. geometryAPI, indexSystem, etc. Additional
  *   arguments for the expression (expressionConfigs).
  */
case class ST_Rotate(
    inputGeom: Expression,
    thetaExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends UnaryVector1ArgExpression[ST_Rotate](
      inputGeom,
      thetaExpr,
      returnsGeometry = true,
      expressionConfig
    ) {

    override def dataType: DataType = inputGeom.dataType

    override def geometryTransform(geometry: MosaicGeometry, arg: Any): Any = {
        geometry.rotate(arg.asInstanceOf[Double])
    }

    override def geometryCodeGen(geometryRef: String, argRef: String, ctx: CodegenContext): (String, String) = {
        val resultRef = ctx.freshName("result")
        val code = s"""$mosaicGeomClass $resultRef = $geometryRef.rotate($argRef);"""
        (code, resultRef)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_Rotate extends WithExpressionInfo {

    override def name: String = "st_rotate"

    override def usage: String = "_FUNC_(expr1, expr2) - Returns rotated geometry."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, b);
          |        POLYGON (...)
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_Rotate](2, expressionConfig)
    }

}
