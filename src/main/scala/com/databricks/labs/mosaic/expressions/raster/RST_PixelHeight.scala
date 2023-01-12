package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

/** Returns the pixel height of the raster. */
case class RST_PixelHeight(path: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_PixelHeight](path, DoubleType, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    /** Returns the pixel height of the raster. */
    override def rasterTransform(raster: MosaicRaster): Any = {
        val scaleY = raster.getRaster.GetGeoTransform()(5)
        val skewX = raster.getRaster.GetGeoTransform()(2)
        // when there is no skew the height is scaleY, but we cant assume 0-only skew
        // skew is not to be confused with rotation
        math.sqrt(scaleY * scaleY + skewX * skewX)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_PixelHeight extends WithExpressionInfo {

    override def name: String = "rst_pixelheight"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns pixel height in the raster.
          |The width is a hypotenuse of a right triangle formed by scaleY and skewX.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |        1.123
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_PixelHeight](1, expressionConfig)
    }

}
