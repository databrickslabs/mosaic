package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

/** Returns the upper left y of the raster. */
case class RST_UpperLeftY(path: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_UpperLeftY](path, DoubleType, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    /** Returns the upper left y of the raster. */
    override def rasterTransform(raster: MosaicRaster): Any = {
        val upperLeftY = raster.getRaster.GetGeoTransform()(3)
        upperLeftY
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_UpperLeftY extends WithExpressionInfo {

    override def name: String = "rst_upperlefty"

    override def usage: String = "_FUNC_(expr1) - Returns upper left y coordinate."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |       1.123
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_UpperLeftY](1, expressionConfig)
    }

}
