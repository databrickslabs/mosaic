package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.operator.CombineAVG
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterArrayExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types.DataType

/** Expression for combining rasters using average of pixels. */
case class RST_CombineAvg(
                             tileExpr: Expression,
                             exprConfig: ExprConfig
) extends RasterArrayExpression[RST_CombineAvg](
      tileExpr,
      returnsRaster = true,
      exprConfig = exprConfig
    )
      with NullIntolerant
      with CodegenFallback {

    // serialize data type
    override def dataType: DataType = {
        RasterTileType(exprConfig.getCellIdType, tileExpr, exprConfig.isRasterUseCheckpoint)
    }

    /** Combines the rasters using average of pixels. */
    override def rasterTransform(tiles: Seq[RasterTile]): Any = {
        val index = if (tiles.map(_.index).groupBy(identity).size == 1) tiles.head.index else null
        val resultType = RasterTile.getRasterType(dataType)
        RasterTile(
            index,
            CombineAVG.compute(tiles.map(_.raster), Option(exprConfig)),
            resultType
        )
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_CombineAvg extends WithExpressionInfo {

    override def name: String = "rst_combineavg"

    override def usage: String =
        """
          |_FUNC_(expr1) - Combine an array of tile tiles using average of pixels.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(array(raster_tile_1, raster_tile_2, raster_tile_3));
          |        {index_id, tile, parent_path, driver}
          |        {index_id, tile, parent_path, driver}
          |        ...
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_CombineAvg](1, exprConfig)
    }

}
