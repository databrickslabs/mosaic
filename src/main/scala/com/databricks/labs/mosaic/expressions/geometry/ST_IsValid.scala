package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.geometry.base.UnaryVectorExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types._

import scala.util.Try

/**
  * SQL Expression that returns true if the geometry is valid.
  * @param inputGeom
  *   Expression that represents the geometry.
  * @param expressionConfig
  *   Mosaic execution context, e.g. geometryAPI, indexSystem, etc. Additional
  *   arguments for the expression (expressionConfigs).
  */
case class ST_IsValid(
    inputGeom: Expression,
    expressionConfig: MosaicExpressionConfig
) extends UnaryVectorExpression[ST_IsValid](inputGeom, returnsGeometry = false, expressionConfig) {

    override def dataType: DataType = BooleanType

    override def geometryTransform(geometry: MosaicGeometry): Any = Try(geometry.isValid).getOrElse(false)

    override def geometryCodeGen(geometryRef: String, ctx: CodegenContext): (String, String) = {
        val resultRef = ctx.freshName("result")
        val mosaicGeometry = mosaicGeometryRef(geometryRef)

        val code = s"""
                      |boolean $resultRef = $mosaicGeometry.isValid();
                      |""".stripMargin

        (code, resultRef)
    }

    override def nullSafeEval(geometryRow: Any): Any = {
        Try { super.nullSafeEval(geometryRow) }.getOrElse(false)
    }

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        nullSafeCodeGen(
          ctx,
          ev,
          leftEval => {
              val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, leftEval, inputGeom.dataType, geometryAPI)
              val (expressionCode, resultRef) = geometryCodeGen(geomInRef, ctx)
              // Invalid format should not crash the execution
              s"""
                 |try {
                 |$inCode
                 |$expressionCode
                 |${ev.value} = $resultRef;
                 |} catch (Exception e) {
                 | ${ev.value} = false;
                 |}
                 |""".stripMargin
          }
        )

}

/** Expression info required for the expression registration for spark SQL. */
object ST_IsValid extends WithExpressionInfo {

    override def name: String = "st_isvalid"

    override def usage: String = "_FUNC_(expr1) - Returns true if geometry is valid."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |        true/false
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_IsValid](1, expressionConfig)
    }

}
