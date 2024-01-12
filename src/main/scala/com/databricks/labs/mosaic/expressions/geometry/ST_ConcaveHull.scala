package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.geometry.base.UnaryVector2ArgExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.adapters.Column
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.DataType

/**
  * Returns the concave hull for a given geometry. It uses lengthRatio and
  * allowHoles to determine the concave hull. lengthRatio is the ratio of the
  * length of the concave hull to the length of the convex hull. If set to 1,
  * this is the same as the convex hull. If set to 0, this is the same as the
  * bounding box. AllowHoles is a boolean that determines whether the concave
  * hull can have holes. If set to true, the concave hull can have holes. If set
  * to false, the concave hull will not have holes. (For PostGIS, the default is
  * false.)
  * @param inputGeom
  *   The input geometry.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  */
case class ST_ConcaveHull(
    inputGeom: Expression,
    lengthRatio: Expression,
    allowHoles: Expression,
    expressionConfig: MosaicExpressionConfig
) extends UnaryVector2ArgExpression[ST_ConcaveHull](
      inputGeom,
      lengthRatio,
      allowHoles,
      returnsGeometry = true,
      expressionConfig
    ) {

    override def dataType: DataType = inputGeom.dataType

    override def geometryTransform(geometry: MosaicGeometry, arg1: Any, arg2: Any): Any = {
        val lenRatio = arg1.asInstanceOf[Double]
        val allowHoles = arg2.asInstanceOf[Boolean]
        geometry.concaveHull(lenRatio, allowHoles)
    }

    override def geometryCodeGen(geometryRef: String, arg1Ref: String, arg2Ref: String, ctx: CodegenContext): (String, String) = {
        val convexHull = ctx.freshName("concaveHull")
        val code = s"""$mosaicGeomClass $convexHull = $geometryRef.concaveHull($arg1Ref, $arg2Ref);"""
        (code, convexHull)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_ConcaveHull extends WithExpressionInfo {

    override def name: String = "st_concavehull"

    override def usage: String = "_FUNC_(expr1, expr2, expr3) - Returns the concave hull for a given geometry with or without holes."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, 0.1, false);
          |        {"POLYGON (( 0 0, 1 0, 1 1, 0 1 ))"}
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = { (children: Seq[Expression]) =>
        GenericExpressionFactory.construct[ST_ConcaveHull](
          Array(children.head, Column(children(1)).cast("double").expr, children(2)),
          expressionConfig
        )
    }

}
