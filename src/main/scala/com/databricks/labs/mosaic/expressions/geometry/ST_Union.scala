package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.geometry.base.BinaryVectorExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.DataType

/**
  * Returns the union of the input geometries.
  * @param leftGeom
  *   The left geometry.
  * @param rightGeom
  *   The right geometry.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  */
case class ST_Union(
    leftGeom: Expression,
    rightGeom: Expression,
    expressionConfig: MosaicExpressionConfig
) extends BinaryVectorExpression[ST_Union](
      leftGeom,
      rightGeom,
      returnsGeometry = true,
      expressionConfig
    ) {

    override def dataType: DataType = leftGeom.dataType

    override def geometryTransform(leftGeometry: MosaicGeometry, rightGeometry: MosaicGeometry): Any = {
        leftGeometry.union(rightGeometry)
    }

    override def geometryCodeGen(leftGeometryRef: String, rightGeometryRef: String, ctx: CodegenContext): (String, String) = {
        val union = ctx.freshName("union")
        val code = s"""$mosaicGeomClass $union = $leftGeometryRef.union($rightGeometryRef);"""
        (code, union)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_Union extends WithExpressionInfo {

    override def name: String = "st_union"

    override def usage: String = "_FUNC_(expr1, expr2) - Returns the union of the input geometries."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, b);
          |        {"POLYGON (( ... ))"}
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_Union](2, expressionConfig)

    }

}
