package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.geometry.base.BinaryVectorExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.{BooleanType, DataType}

/**
  * Returns true if leftGeom contains rightGeom.
  * @param leftGeom
  *   The left geometry.
  * @param rightGeom
  *   The right geometry.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  */
case class ST_Contains(
    leftGeom: Expression,
    rightGeom: Expression,
    expressionConfig: MosaicExpressionConfig
) extends BinaryVectorExpression[ST_Contains](
      leftGeom,
      rightGeom,
      returnsGeometry = false,
      expressionConfig
    ) {

    override def dataType: DataType = BooleanType

    override def geometryTransform(leftGeometry: MosaicGeometry, rightGeometry: MosaicGeometry): Any = {
        leftGeometry.contains(rightGeometry)
    }

    override def geometryCodeGen(leftGeometryRef: String, rightGeometryRef: String, ctx: CodegenContext): (String, String) = {
        val contains = ctx.freshName("contains")
        val code = s"""boolean $contains = $leftGeometryRef.contains($rightGeometryRef);"""
        (code, contains)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_Contains extends WithExpressionInfo {

    override def name: String = "st_contains"

    override def usage: String = "_FUNC_(expr1, expr2) - Returns true if expr1 contains expr2."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(A, B);
          |        true
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_Contains](2, expressionConfig)
    }

}
