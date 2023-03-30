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
  * Returns the envelope for a given geometry.
  * @param inputGeom
  *   The input geometry.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  */
case class ST_Envelope(
    inputGeom: Expression,
    expressionConfig: MosaicExpressionConfig
) extends UnaryVectorExpression[ST_Envelope](inputGeom, returnsGeometry = true, expressionConfig) {

    override def dataType: DataType = inputGeom.dataType

    override def geometryTransform(geometry: MosaicGeometry): Any = geometry.envelope

    override def geometryCodeGen(geometryRef: String, ctx: CodegenContext): (String, String) = {
        val resultRef = ctx.freshName("result")
        val code = s"""$mosaicGeomClass $resultRef = $geometryRef.envelope();"""
        (code, resultRef)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_Envelope extends WithExpressionInfo {

    override def name: String = "st_envelope"

    override def usage: String = "_FUNC_(expr1) - Returns the envelope as a geometry."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |        LINESTRING(....)
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_Envelope](1, expressionConfig)
    }

}
