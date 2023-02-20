package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.geometry.base.UnaryVectorExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.{DataType, DoubleType}

/**
  * SQL expression that returns Y coordinate of the input geometry. If the
  * geometry is a point, the Y coordinate of the point is returned. If the
  * geometry is any other type, the Y coordinate of the centroid of the geometry
  * is returned.
  *
  * @param inputGeom
  *   Expression containing the geometry.
  * @param expressionConfig
  *   Mosaic execution context, e.g. geometryAPI, indexSystem, etc. Additional
  *   arguments for the expression (expressionConfigs).
  */
case class ST_Y(
    inputGeom: Expression,
    expressionConfig: MosaicExpressionConfig
) extends UnaryVectorExpression[ST_Y](inputGeom, returnsGeometry = false, expressionConfig) {

    override def dataType: DataType = DoubleType

    override def geometryTransform(geometry: MosaicGeometry): Any = geometry.getCentroid.getY

    override def geometryCodeGen(geometryRef: String, ctx: CodegenContext): (String, String) = {
        val resultRef = ctx.freshName("result")
        val code = s"""double $resultRef = $geometryRef.getCentroid().getY();"""
        (code, resultRef)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_Y extends WithExpressionInfo {

    override def name: String = "st_y"

    override def usage: String =
        "_FUNC_(expr1) - Returns y coordinate of a point or y coordinate of the centroid if the geometry isnt a point."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |        12.3
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_Y](1, expressionConfig)
    }

}
