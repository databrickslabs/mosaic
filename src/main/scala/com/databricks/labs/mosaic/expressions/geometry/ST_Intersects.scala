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
  * Returns true if leftGeom intersects rightGeom.
  * @param leftGeom
  *   The left geometry.
  * @param rightGeom
  *   The right geometry.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  */
case class ST_Intersects(
    leftGeom: Expression,
    rightGeom: Expression,
    expressionConfig: MosaicExpressionConfig
) extends BinaryVectorExpression[ST_Intersects](
      leftGeom,
      rightGeom,
      returnsGeometry = false,
      expressionConfig
    ) {

    override def dataType: DataType = BooleanType

    override def geometryTransform(leftGeometry: MosaicGeometry, rightGeometry: MosaicGeometry): Any = {
        leftGeometry.intersects(rightGeometry)
    }

    override def geometryCodeGen(leftGeometryRef: String, rightGeometryRef: String, ctx: CodegenContext): (String, String) = {
        val intersects = ctx.freshName("intersects")
        val code = s"""boolean $intersects = $leftGeometryRef.intersects($rightGeometryRef);"""
        (code, intersects)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_Intersects extends WithExpressionInfo {

    override def name: String = "st_intersects"

    override def usage: String = "_FUNC_(expr1, expr2) - Returns true if expr1 intersects expr2."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, b);
          |        POLYGON(...)
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_Intersects](2, expressionConfig)
    }

}
