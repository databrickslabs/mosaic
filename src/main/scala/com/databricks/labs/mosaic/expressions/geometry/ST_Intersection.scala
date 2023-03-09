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
  * Returns the intersection of leftGeom and rightGeom.
  * @param leftGeom
  *   The left geometry.
  * @param rightGeom
  *   The right geometry.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  */
case class ST_Intersection(
    leftGeom: Expression,
    rightGeom: Expression,
    expressionConfig: MosaicExpressionConfig
) extends BinaryVectorExpression[ST_Intersection](
      leftGeom,
      rightGeom,
      returnsGeometry = true,
      expressionConfig
    ) {

    override def dataType: DataType = leftGeom.dataType

    override def geometryTransform(leftGeometry: MosaicGeometry, rightGeometry: MosaicGeometry): Any = {
        leftGeometry.intersection(rightGeometry)
    }

    override def geometryCodeGen(leftGeometryRef: String, rightGeometryRef: String, ctx: CodegenContext): (String, String) = {
        val intersection = ctx.freshName("intersection")
        val code = s"""$mosaicGeomClass $intersection = $leftGeometryRef.intersection($rightGeometryRef);"""
        (code, intersection)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_Intersection extends WithExpressionInfo {

    override def name: String = "st_intersection"

    override def usage: String = "_FUNC_(expr1, expr2) - Returns the intersection of expr1 and expr2."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(A, B);
          |        POLYGON (...)
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_Intersection](2, expressionConfig)
    }

}
