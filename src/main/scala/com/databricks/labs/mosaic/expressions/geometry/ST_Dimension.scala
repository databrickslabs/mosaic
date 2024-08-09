package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.geometry.base.UnaryVectorExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.{DataType, IntegerType}

/**
  * SQL expression that returns the dimension of the input geometry.
  * @param inputGeom
  *   Expression containing the geometry.
  * @param exprConfig
  *   Mosaic execution context, e.g. geometryAPI, indexSystem, etc. Additional
  *   arguments for the expression (expressionConfigs).
  */
case class ST_Dimension(
                           inputGeom: Expression,
                           exprConfig: ExprConfig
) extends UnaryVectorExpression[ST_Dimension](inputGeom, returnsGeometry = false, exprConfig) {

    override def dataType: DataType = IntegerType

    override def geometryTransform(geometry: MosaicGeometry): Any = geometry.getDimension

    override def geometryCodeGen(geometryRef: String, ctx: CodegenContext): (String, String) = {
        val resultRef = ctx.freshName("result")
        val code = s"""int $resultRef = $geometryRef.getDimension();"""
        (code, resultRef)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_Dimension extends WithExpressionInfo {

    override def name: String = "st_dimension"

    override def usage: String = "_FUNC_(expr1) - Returns dimension of the geometry."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |        1
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_Dimension](1, exprConfig)
    }

}
