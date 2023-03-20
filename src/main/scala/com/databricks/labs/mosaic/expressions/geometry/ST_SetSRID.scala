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
  * SQL expression that returns the input geometry buffered by the radius.
  * @param inputGeom
  *   Expression containing the geometry.
  * @param sridExpr
  *   The SRID to be set for the geometry.
  * @param expressionConfig
  *   Mosaic execution context, e.g. geometryAPI, indexSystem, etc. Additional
  *   arguments for the expression (expressionConfigs).
  */
case class ST_SetSRID(
    inputGeom: Expression,
    sridExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends UnaryVector1ArgExpression[ST_SetSRID](
      inputGeom,
      sridExpr,
      returnsGeometry = true,
      expressionConfig
    )
      with RequiresCRS {

    override def dataType: DataType = inputGeom.dataType

    override def geometryTransform(geometry: MosaicGeometry, arg: Any): Any = {
        geometry.setSpatialReference(arg.asInstanceOf[Int])
        geometry
    }

    override def geometryCodeGen(geometryRef: String, argRef: String, ctx: CodegenContext): (String, String) = {
        val resultRef = ctx.freshName("result")
        // Side effect: sets the spatial reference of the geometry
        // return the same input reference
        val code = s"""$geometryRef.setSpatialReference($argRef);"""
        (code, geometryRef)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_SetSRID extends WithExpressionInfo {

    override def name: String = "st_setsrid"

    override def usage: String = "_FUNC_(expr1, expr2) - Returns geometry with SRID set."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, b);
          |        POLYGON (...)
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_SetSRID](2, expressionConfig)
    }

}
