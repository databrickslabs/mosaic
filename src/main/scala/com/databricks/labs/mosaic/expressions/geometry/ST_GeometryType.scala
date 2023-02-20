package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.geometry.base.UnaryVectorExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.util.Locale

/**
  * SQL Expression for returning the geometry type of a geometry.
  * @param inputGeom
  *   The input geometry expression.
  * @param expressionConfig
  *   Mosaic execution context, e.g. the geometry API, index system, etc.
  *   Additional arguments for the expression (expressionConfigs).
  */
case class ST_GeometryType(
    inputGeom: Expression,
    expressionConfig: MosaicExpressionConfig
) extends UnaryVectorExpression[ST_GeometryType](inputGeom, returnsGeometry = false, expressionConfig) {

    override def dataType: DataType = StringType

    override def geometryTransform(geometry: MosaicGeometry): Any = UTF8String.fromString(geometry.getGeometryType.toUpperCase(Locale.ROOT))

    override def geometryCodeGen(geometryRef: String, ctx: CodegenContext): (String, String) = {
        val resultRef = ctx.freshName("result")
        val geometryType = ctx.freshName("geometryType")
        val locale = "java.util.Locale.ROOT"
        val javaStringClass = CodeGenerator.javaType(StringType)

        val code = s"""
                      |String $geometryType = $geometryRef.getGeometryType().toUpperCase($locale);
                      |$javaStringClass $resultRef = $javaStringClass.fromString($geometryType);
                      |""".stripMargin

        (code, resultRef)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_GeometryType extends WithExpressionInfo {

    override def name: String = "st_geometrytype"

    override def usage: String = "_FUNC_(expr1) - Returns the type of the geometry."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |        POINT
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_GeometryType](1, expressionConfig)
    }

}
