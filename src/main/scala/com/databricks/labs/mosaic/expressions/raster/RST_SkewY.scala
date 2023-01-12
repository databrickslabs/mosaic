package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

/** Returns the skew y of the raster. */
case class RST_SkewY(path: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_SkewY](path, DoubleType, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    /** Returns the skew y of the raster. */
    override def rasterTransform(raster: MosaicRaster): Any = {
        val skewY = raster.getRaster.GetGeoTransform()(4)
        skewY
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_SkewY extends WithExpressionInfo {

    override def name: String = "rst_skewy"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns skew Y in the raster.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |        1.123
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_SkewY](1, expressionConfig)
    }

}
