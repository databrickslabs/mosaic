package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

/** Returns the upper left x of the raster. */
case class RST_PixelCount(rasterExpr: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_PixelCount](rasterExpr, returnsRaster = false, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = ArrayType(LongType)

    /** Returns the upper left x of the raster. */
    override def rasterTransform(tile: MosaicRasterTile): Any = {
        val bandCount = tile.raster.raster.GetRasterCount()
        val pixelCount = (1 to bandCount).map(tile.raster.getBand(_).pixelCount)
        ArrayData.toArrayData(pixelCount.toArray)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_PixelCount extends WithExpressionInfo {

    override def name: String = "rst_pixelcount"

    override def usage: String = "_FUNC_(expr1) - Returns an array containing valid pixel count values for each band."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |       [12, 212, 313]
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_PixelCount](1, expressionConfig)
    }

}
