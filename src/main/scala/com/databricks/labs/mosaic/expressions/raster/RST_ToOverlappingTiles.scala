package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.operator.retile.OverlappingTiles
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterGeneratorExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}

/**
  * Returns a set of new rasters which are the result of a rolling window over
  * the input raster.
  */
case class RST_ToOverlappingTiles(
    rasterExpr: Expression,
    tileWidthExpr: Expression,
    tileHeightExpr: Expression,
    overlapExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends RasterGeneratorExpression[RST_ToOverlappingTiles](rasterExpr, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    /**
      * Returns a set of new rasters which are the result of a rolling window
      * over the input raster.
      */
    override def rasterGenerator(tile: MosaicRasterTile): Seq[MosaicRasterTile] = {
        val tileWidthValue = tileWidthExpr.eval().asInstanceOf[Int]
        val tileHeightValue = tileHeightExpr.eval().asInstanceOf[Int]
        val overlapValue = overlapExpr.eval().asInstanceOf[Int]
        OverlappingTiles.reTile(tile, tileWidthValue, tileHeightValue, overlapValue)
    }

    override def children: Seq[Expression] = Seq(rasterExpr, tileWidthExpr, tileHeightExpr, overlapExpr)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_ToOverlappingTiles extends WithExpressionInfo {

    override def name: String = "rst_tooverlappingtiles"

    override def usage: String =
        """
          |_FUNC_(expr1, expr2, expr3, expr4) - Returns a set of new raster tiles with the specified tile size (tileWidth x tileHeight).
          |                                     The tiles will overlap by the specified amount.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile, 256, 256, 10);
          |        {index_id, raster_tile, tile_width, tile_height}
          |        {index_id, raster_tile, tile_width, tile_height}
          |        ...
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_ToOverlappingTiles](4, expressionConfig)
    }

}
