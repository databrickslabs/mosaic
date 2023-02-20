package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.crs.CRSBoundsProvider
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.geometry.base.UnaryVector2ArgExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.{BooleanType, DataType}
import org.apache.spark.unsafe.types.UTF8String

import java.util.Locale

/**
  * SQL Expression for returning true if all vertices of the geometry are within
  * CRS bounds.
  * @param inputGeom
  *   The input geometry expression.
  * @param crsCode
  *   The input crs code expression.
  * @param which
  *   The input which expression, either bounds or reprojected_bounds .
  * @param expressionConfig
  *   Mosaic execution context, e.g. the geometry API, index system, etc.
  *   Additional arguments for the expression (expressionConfigs).
  */
case class ST_HasValidCoordinates(
    inputGeom: Expression,
    crsCode: Expression,
    which: Expression,
    expressionConfig: MosaicExpressionConfig
) extends UnaryVector2ArgExpression[ST_HasValidCoordinates](
      inputGeom,
      crsCode,
      which,
      returnsGeometry = false,
      expressionConfig
    ) {

    @transient
    val crsBoundsProvider: CRSBoundsProvider = CRSBoundsProvider(geometryAPI)

    override def dataType: DataType = BooleanType

    override def geometryTransform(geometry: MosaicGeometry, arg1: Any, arg2: Any): Any = {
        val crsCode = arg1.asInstanceOf[UTF8String].toString
        val which = arg2.asInstanceOf[UTF8String].toString.toLowerCase(Locale.ROOT)
        geometry.hasValidCoords(crsBoundsProvider, crsCode, which)
    }

    override def geometryCodeGen(geometryRef: String, arg1Ref: String, arg2Ref: String, ctx: CodegenContext): (String, String) = {
        val resultRef = ctx.freshName("result")
        val crsBoundsProviderRef = ctx.freshName("crsBoundsProvider")
        val geometryAPIRef = ctx.freshName("geometryAPI")

        ctx.addImmutableStateIfNotExists(
          CRSBoundsProviderClass,
          crsBoundsProviderRef
        )

        ctx.addPartitionInitializationStatement(
          s"""
             |final $geometryAPIClass $geometryAPIRef = $geometryAPIClass.apply("${geometryAPI.name}");
             |$crsBoundsProviderRef = $CRSBoundsProviderClass.apply($geometryAPIRef);
             |""".stripMargin
        )

        val code = s"""boolean $resultRef = $geometryRef.hasValidCoords($crsBoundsProviderRef, $arg1Ref.toString(), $arg2Ref.toString());"""

        (code, resultRef)

    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_HasValidCoordinates extends WithExpressionInfo {

    override def name: String = "st_hasvalidcoordinates"

    override def usage: String =
        "_FUNC_(expr1, expr2, expr3) - Checks if all points in geometry have valid coordinates with respect to provided crs code and the type of bounds."

    override def example: String = """
        > SELECT _FUNC_(geom, 'EPSG:4326', 'bounds');
        true
    """

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_HasValidCoordinates](3, expressionConfig)
    }

}
