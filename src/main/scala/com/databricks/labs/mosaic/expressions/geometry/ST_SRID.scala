package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.geometry.base.{RequiresCRS, UnaryVectorExpression}
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types._

/**
  * Returns spatial reference ID of the geometry.
  * @param inputGeom
  *   The geometry to get the spatial reference ID from.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  */
case class ST_SRID(
    inputGeom: Expression,
    expressionConfig: MosaicExpressionConfig
) extends UnaryVectorExpression[ST_SRID](inputGeom, returnsGeometry = false, expressionConfig)
      with RequiresCRS {

    override def dataType: DataType = IntegerType

    override def geometryTransform(geometry: MosaicGeometry): Any = geometry.getSpatialReference

    override def geometryCodeGen(geometryRef: String, ctx: CodegenContext): (String, String) = {
        val resultRef = ctx.freshName("result")
        val code = s"""int $resultRef = $geometryRef.getSpatialReference();"""
        (code, resultRef)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_SRID extends WithExpressionInfo {

    override def name: String = "st_srid"

    override def usage: String = "_FUNC_(expr1) - Returns spatial reference ID of the geometry."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |        27700
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_SRID](1, expressionConfig)
    }

}
