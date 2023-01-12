package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

/** Returns the pixel width of the raster. */
case class RST_PixelWidth(path: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_PixelWidth](path, DoubleType, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    /** Returns the pixel width of the raster. */
    override def rasterTransform(raster: MosaicRaster): Any = {
        val scaleX = raster.getRaster.GetGeoTransform()(1)
        val skewY = raster.getRaster.GetGeoTransform()(4)
        // when there is no skew width is scaleX, but we cant assume 0-only skew
        // skew is not to be confused with rotation
        math.sqrt(scaleX * scaleX + skewY * skewY)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_PixelWidth extends WithExpressionInfo {

    override def name: String = "rst_pixelwidth"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns pixel width in the raster.
          |The width is a hypotenuse of a right triangle formed by scaleX and skewY.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |        1.123
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_PixelWidth](2, expressionConfig)
    }

}
