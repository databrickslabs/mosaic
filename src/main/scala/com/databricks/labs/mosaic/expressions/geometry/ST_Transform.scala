package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.geometry.base.{RequiresCRS, UnaryVector1ArgExpression}
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types._

/**
  * SQL expression that returns the input geometry transformed to provided SRID.
  * @param inputGeom
  *   Expression containing the geometry.
  * @param sridExpr
  *   Expression containing the SRID.
  * @param expressionConfig
  *   Mosaic execution context, e.g. geometryAPI, indexSystem, etc. Additional
  *   arguments for the expression (expressionConfigs).
  */
case class ST_Transform(
    inputGeom: Expression,
    sridExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends UnaryVector1ArgExpression[ST_Transform](
      inputGeom,
      sridExpr,
      returnsGeometry = true,
      expressionConfig
    )
      with RequiresCRS {

    override def dataType: DataType = inputGeom.dataType

    override def geometryTransform(geometry: MosaicGeometry, arg: Any): Any = {
        checkEncoding(inputGeom.dataType)
        geometry.transformCRSXY(arg.asInstanceOf[Int])
    }

    override def geometryCodeGen(geometryRef: String, argRef: String, ctx: CodegenContext): (String, String) = {
        val resultRef = ctx.freshName("result")
        checkEncoding(inputGeom.dataType)
        val code = s"""$mosaicGeomClass $resultRef = $geometryRef.transformCRSXY($argRef);"""
        (code, resultRef)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_Transform extends WithExpressionInfo {

    override def name: String = "st_transform"

    override def usage: String = "_FUNC_(expr1, expr2) - Returns transformed geometry to SRID."

    override def example: String =
        """
          |     Examples:
          |         > SELECT _FUNC_(a, b);
          |           POLYGON (...)
          |""".stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_Transform](2, expressionConfig)
    }

}
