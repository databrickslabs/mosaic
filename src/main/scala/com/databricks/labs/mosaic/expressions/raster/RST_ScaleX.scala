package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.gdal_raster.RasterCleaner
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.{FunctionBuilder, expressions}
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

/** Returns the scale x of the raster. */
case class RST_ScaleX(raster: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_ScaleX](raster, DoubleType, returnsRaster = false, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    /** Returns the scale x of the raster. */
    override def rasterTransform(raster: MosaicRaster): Any = {
        val scaleX = raster.getRaster.GetGeoTransform()(1)
        RasterCleaner.dispose(raster)
        scaleX
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_ScaleX extends WithExpressionInfo {

    override def name: String = "rst_scalex"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns scale X in the raster.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |        1.123
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_ScaleX](1, expressionConfig)
    }

}
