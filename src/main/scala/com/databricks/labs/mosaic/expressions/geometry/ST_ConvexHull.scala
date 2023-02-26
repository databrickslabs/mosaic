package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.geometry.base.UnaryVectorExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.DataType

/**
  * Returns the convex hull for a given geometry.
  * @param inputGeom
  *   The input geometry.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  */
case class ST_ConvexHull(
    inputGeom: Expression,
    expressionConfig: MosaicExpressionConfig
) extends UnaryVectorExpression[ST_ConvexHull](
      inputGeom,
      returnsGeometry = true,
      expressionConfig
    ) {

    override def dataType: DataType = inputGeom.dataType

    override def geometryTransform(geometry: MosaicGeometry): Any = {
        geometry.convexHull
    }

    override def geometryCodeGen(geometryRef: String, ctx: CodegenContext): (String, String) = {
        val convexHull = ctx.freshName("convexHull")
        val code = s"""$mosaicGeomClass $convexHull = $geometryRef.convexHull();"""
        (code, convexHull)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_ConvexHull extends WithExpressionInfo {

    override def name: String = "st_convexhull"

    override def usage: String = "_FUNC_(expr1) - Returns the convex hull for a given geometry."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |        {"POLYGON (( 0 0, 1 0, 1 1, 0 1 ))"}
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_ConvexHull](1, expressionConfig)
    }

}
