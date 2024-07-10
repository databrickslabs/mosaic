package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.operator.retile.ReTile
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterGeneratorExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types.DataType

/**
  * Returns a set of new rasters with the specified tile size (tileWidth x
  * tileHeight).
  * - always uses the checkpoint location.
  */
case class RST_ReTile(
                         rasterExpr: Expression,
                         tileWidthExpr: Expression,
                         tileHeightExpr: Expression,
                         exprConfig: ExprConfig
) extends RasterGeneratorExpression[RST_ReTile](rasterExpr, exprConfig)
      with NullIntolerant
      with CodegenFallback {

    /** @return provided tile data type (assumes that was handled for checkpointing.)*/
    override def dataType: DataType = {
        // 0.4.3 changed from `rasterExpr.rasterType`
        RasterTileType(exprConfig.getCellIdType, rasterExpr, useCheckpoint = true) // always use checkpoint
    }

    /**
      * Returns a set of new rasters with the specified tile size (tileWidth x
      * tileHeight).
      */
    override def rasterGenerator(tile: RasterTile): Seq[RasterTile] = {
        val tileWidthValue = tileWidthExpr.eval().asInstanceOf[Int]
        val tileHeightValue = tileHeightExpr.eval().asInstanceOf[Int]
        ReTile.reTile(tile, tileWidthValue, tileHeightValue, Option(exprConfig))
    }

    override def children: Seq[Expression] = Seq(rasterExpr, tileWidthExpr, tileHeightExpr)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_ReTile extends WithExpressionInfo {

    override def name: String = "rst_retile"

    override def usage: String =
        """
          |_FUNC_(expr1, expr2, expr3) - Returns a set of new tile tile with the specified size (tileWidth x tileHeight).
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile, 256, 256);
          |        {index_id, raster_tile, tile_width, tile_height}
          |        {index_id, raster_tile, tile_width, tile_height}
          |        {index_id, raster_tile, tile_width, tile_height}
          |        ...
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_ReTile](3, exprConfig)
    }

}
