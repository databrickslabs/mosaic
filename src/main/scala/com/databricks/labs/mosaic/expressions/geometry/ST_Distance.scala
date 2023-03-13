package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.geometry.base.BinaryVectorExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.{DataType, DoubleType}

/**
  * Returns the Euclidean distance between leftGeom and rightGeom.
  * @param leftGeom
  *   The left geometry.
  * @param rightGeom
  *   The right geometry.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  */
case class ST_Distance(
    leftGeom: Expression,
    rightGeom: Expression,
    expressionConfig: MosaicExpressionConfig
) extends BinaryVectorExpression[ST_Distance](
      leftGeom,
      rightGeom,
      returnsGeometry = false,
      expressionConfig
    ) {

    override def dataType: DataType = DoubleType

    override def geometryTransform(leftGeometry: MosaicGeometry, rightGeometry: MosaicGeometry): Any = {
        leftGeometry.distance(rightGeometry)
    }

    override def geometryCodeGen(leftGeometryRef: String, rightGeometryRef: String, ctx: CodegenContext): (String, String) = {
        val distance = ctx.freshName("distance")
        val code = s"""double $distance = $leftGeometryRef.distance($rightGeometryRef);"""
        (code, distance)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_Distance extends WithExpressionInfo {

    override def name: String = "st_distance"

    override def usage: String = "_FUNC_(expr1, expr2) - Returns the Euclidean distance between expr1 and expr2."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(A, B);
          |        15.2512
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_Distance](2, expressionConfig)
    }

}
