package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.WithExpressionInfo
import com.databricks.labs.mosaic.expressions.geometry.base.UnaryVector1ArgExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.adapters.Column
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.DataType

/**
  * SQL expression that returns the input geometry buffered by the radius.
  * @param inputGeom
  *   Expression containing the geometry.
  * @param radiusExpr
  *   The radius of the buffer.
  * @param expressionConfig
  *   Mosaic execution context, e.g. geometryAPI, indexSystem, etc. Additional
  *   arguments for the expression (expressionConfigs).
  */
case class ST_Buffer(
    inputGeom: Expression,
    radiusExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends UnaryVector1ArgExpression[ST_Buffer](inputGeom, radiusExpr, returnsGeometry = true, expressionConfig) {

    override def dataType: DataType = inputGeom.dataType

    override def geometryTransform(geometry: MosaicGeometry, arg: Any): Any = {
        val radius = arg.asInstanceOf[Double]
        geometry.buffer(radius)
    }

    override def geometryCodeGen(geometryRef: String, argRef: String, ctx: CodegenContext): (String, String) = {
        val resultRef = ctx.freshName("result")
        val code = s"""$mosaicGeomClass $resultRef = $geometryRef.buffer($argRef);"""
        (code, resultRef)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_Buffer extends WithExpressionInfo {

    override def name: String = "st_buffer"

    override def usage: String = "_FUNC_(expr1, expr2) - Returns buffered geometry."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, b);
          |        POLYGON (...)
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = { (children: Seq[Expression]) =>
        ST_Buffer(children.head, Column(children(1)).cast("double").expr, expressionConfig)
    }

}
