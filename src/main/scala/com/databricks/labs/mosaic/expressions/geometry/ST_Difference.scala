package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.geometry.base.BinaryVectorExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.DataType

/**
  * Returns the difference of the two geometries.
  * @param leftGeom
  *   The left geometry.
  * @param rightGeom
  *   The right geometry.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  */
case class ST_Difference(
    leftGeom: Expression,
    rightGeom: Expression,
    expressionConfig: MosaicExpressionConfig
) extends BinaryVectorExpression[ST_Difference](
      leftGeom,
      rightGeom,
      returnsGeometry = true,
      expressionConfig
    ) {

    override def dataType: DataType = leftGeom.dataType

    override def geometryTransform(leftGeometry: MosaicGeometry, rightGeometry: MosaicGeometry): Any = {
        leftGeometry.difference(rightGeometry)
    }

    override def geometryCodeGen(leftGeometryRef: String, rightGeometryRef: String, ctx: CodegenContext): (String, String) = {
        val difference = ctx.freshName("difference")
        val code = s"""$mosaicGeomClass $difference = $leftGeometryRef.difference($rightGeometryRef);"""
        (code, difference)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_Difference extends WithExpressionInfo {

    override def name: String = "st_difference"

    override def usage: String = "_FUNC_(expr1, expr2) - Returns the difference of the two geometries."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, b);
          |        {"POLYGON (( ... ))"}
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_Difference](2, expressionConfig)
    }

}
