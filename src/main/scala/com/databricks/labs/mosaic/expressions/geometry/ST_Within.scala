package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.geometry.base.BinaryVectorExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.{BooleanType, DataType}

/**
  * Returns true if leftGeom is within rightGeom.
  * @param leftGeom
  *   The left geometry.
  * @param rightGeom
  *   The right geometry.
  * @param exprConfig
  *   Additional arguments for the expression (expressionConfigs).
  */
case class ST_Within(
                        leftGeom: Expression,
                        rightGeom: Expression,
                        exprConfig: ExprConfig
) extends BinaryVectorExpression[ST_Within](
      leftGeom,
      rightGeom,
      returnsGeometry = false,
      exprConfig
    ) {

    override def dataType: DataType = BooleanType

    override def geometryTransform(leftGeometry: MosaicGeometry, rightGeometry: MosaicGeometry): Any = {
        leftGeometry.within(rightGeometry)
    }

    override def geometryCodeGen(leftGeometryRef: String, rightGeometryRef: String, ctx: CodegenContext): (String, String) = {
        val within = ctx.freshName("within")
        val code = s"""boolean $within = $leftGeometryRef.within($rightGeometryRef);"""
        (code, within)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_Within extends WithExpressionInfo {

    override def name: String = "st_within"

    override def usage: String = "_FUNC_(expr1, expr2) - Returns true if expr1 is within expr2."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(A, B);
          |        true
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_Within](2, exprConfig)
    }

}
