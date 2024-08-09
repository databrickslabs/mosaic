package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.geometry.base.UnaryVectorExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types._

/**
  * SQL expression that returns the centroid of the input geometry.
  * @param inputGeom
  *   Expression containing the geometry.
  * @param exprConfig
  *   Mosaic execution context, e.g. geometryAPI, indexSystem, etc. Additional
  *   arguments for the expression (expressionConfigs).
  */
case class ST_Centroid(
                          inputGeom: Expression,
                          exprConfig: ExprConfig
) extends UnaryVectorExpression[ST_Centroid](inputGeom, returnsGeometry = true, exprConfig) {

    override def dataType: DataType = inputGeom.dataType

    override def geometryTransform(geometry: MosaicGeometry): Any = geometry.getCentroid

    override def geometryCodeGen(geometryRef: String, ctx: CodegenContext): (String, String) = {
        val resultRef = ctx.freshName("result")
        val code = s"""$mosaicGeomClass $resultRef = $geometryRef.getCentroid();"""
        (code, resultRef)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_Centroid extends WithExpressionInfo {

    override def name: String = "st_centroid"

    override def usage: String = "_FUNC_(expr1) - Returns centroid point geometry."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |        POINT(1.1, 2.2)
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_Centroid](1, exprConfig)
    }

    def legacyInfo(database: Option[String], name: String): ExpressionInfo =
        new ExpressionInfo(
          this.getClass.getCanonicalName,
          database.getOrElse(this.database.orNull),
          name,
          usage,
          "",
          example,
          "",
          group,
          "1.0",
          "",
          "built-in"
        )

}
