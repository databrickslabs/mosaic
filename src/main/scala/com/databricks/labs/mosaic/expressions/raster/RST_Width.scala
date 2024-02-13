package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types._

/** Returns the width of the raster. */
case class RST_Width(raster: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_Width](raster, returnsRaster = false, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = IntegerType

    /** Returns the width of the raster. */
    override def rasterTransform(tile: MosaicRasterTile): Any = tile.getRaster.xSize

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Width extends WithExpressionInfo {

    override def name: String = "rst_width"

    override def usage: String = "_FUNC_(expr1) - Returns width of the raster tile."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |       512
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Width](1, expressionConfig)
    }

}
