package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.operator.pixel.PixelCombineRasters
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterArray2ArgExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

/** Expression for combining rasters using average of pixels. */
case class RST_DerivedBand(
    tileExpr: Expression,
    pythonFuncExpr: Expression,
    funcNameExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends RasterArray2ArgExpression[RST_DerivedBand](
      tileExpr,
      pythonFuncExpr,
      funcNameExpr,
      returnsRaster = true,
      expressionConfig = expressionConfig
    )
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = RasterTileType(expressionConfig.getCellIdType, tileExpr)

    /** Combines the rasters using average of pixels. */
    override def rasterTransform(tiles: Seq[MosaicRasterTile], arg1: Any, arg2: Any): Any = {
        val pythonFunc = arg1.asInstanceOf[UTF8String].toString
        val funcName = arg2.asInstanceOf[UTF8String].toString
        val index = if (tiles.map(_.getIndex).groupBy(identity).size == 1) tiles.head.getIndex else null
        MosaicRasterTile(
          index,
          PixelCombineRasters.combine(tiles.map(_.getRaster), pythonFunc, funcName)
        )
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_DerivedBand extends WithExpressionInfo {

    override def name: String = "rst_derivedband"

    override def usage: String =
        """
          |_FUNC_(expr1) - Combine an array of raster tiles using provided python function.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(
          |             array(raster_tile_1, raster_tile_2, raster_tile_3),
          |             'def average(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
          |                 out_ar[:] = np.sum(in_ar, axis=0) / len(in_ar)
          |             ',
          |             'average'
          |       );
          |        {index_id, raster, parent_path, driver}
          |        {index_id, raster, parent_path, driver}
          |        ...
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_DerivedBand](3, expressionConfig)
    }

}
