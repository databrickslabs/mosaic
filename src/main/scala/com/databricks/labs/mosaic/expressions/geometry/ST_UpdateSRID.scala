package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.WithExpressionInfo
import com.databricks.labs.mosaic.expressions.geometry.base.UnaryVector2ArgExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.adapters.Column
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.DataType

/**
  * SQL expression that returns area of the input geometry.
  *
  * @param inputGeom
  *   Expression containing the geometry.
  * @param srcSRIDExpr
  *   Expression containing the source SRID.
  * @param destSRIDExpr
  *   Expression containing the destination SRID.
  * @param expressionConfig
  *   Mosaic execution context, e.g. geometryAPI, indexSystem, etc. Additional
  *   arguments for the expression (expressionConfigs).
  */
case class ST_UpdateSRID(
    inputGeom: Expression,
    srcSRIDExpr: Expression,
    destSRIDExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends UnaryVector2ArgExpression[ST_UpdateSRID](
      inputGeom,
      srcSRIDExpr,
      destSRIDExpr,
      returnsGeometry = true,
      expressionConfig
    ) {

    override def dataType: DataType = inputGeom.dataType

    override def geometryTransform(geometry: MosaicGeometry, srcSRID: Any, destSRID: Any): Any = {
        geometry.transformCRSXY(destSRID.asInstanceOf[Int], srcSRID.asInstanceOf[Int])
    }

    override def geometryCodeGen(geometryRef: String, srcSRIDRef: String, destSRIDRef: String, ctx: CodegenContext): (String, String) = {
        val resultRef = ctx.freshName("result")
        val code = s"""$mosaicGeomClass $resultRef = (($mosaicGeomClass) $geometryRef.transformCRSXY($destSRIDRef, $srcSRIDRef));"""
        (code, resultRef)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_UpdateSRID extends WithExpressionInfo {

    override def name: String = "st_updatesrid"

    override def usage: String = "_FUNC_(expr1, expr2, expr3) - Transforms the geometry from SrcSRID to DestSRID."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, b, c);
          |        POINT(...)
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = { (exprs: Seq[Expression]) =>
        ST_UpdateSRID(exprs(0), Column(exprs(1)).cast("int").expr, Column(exprs(2)).cast("int").expr, expressionConfig)
    }

}
