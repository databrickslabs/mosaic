package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.operator.merge.MergeRasters
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterArrayExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types.DataType

/** Returns a tile that is a result of merging an array of rasters. */
case class RST_Merge(
                        rastersExpr: Expression,
                        exprConfig: ExprConfig
) extends RasterArrayExpression[RST_Merge](
      rastersExpr,
      returnsRaster = true,
      exprConfig = exprConfig
    )
      with NullIntolerant
      with CodegenFallback {

    GDAL.enable(exprConfig)

    // serialize data type
    override def dataType: DataType = {
        RasterTileType(exprConfig.getCellIdType, rastersExpr, exprConfig.isRasterUseCheckpoint)
    }

    /**
      * Merges an array of rasters.
      * @param tiles
      *   The rasters to be used.
      * @return
      *   The merged tile.
      */
    override def rasterTransform(tiles: Seq[RasterTile]): Any = {
        val index = if (tiles.map(_.index).groupBy(identity).size == 1) tiles.head.index else null
        val mergeRaster = MergeRasters.merge(tiles.map(_.raster), Option(exprConfig))

        tiles.head.copy(
          raster = mergeRaster,
          index = index
        )
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Merge extends WithExpressionInfo {

    override def name: String = "rst_merge"

    override def usage: String =
        """
          |_FUNC_(expr1) - Merge (mosaic) an array of tile tile columns.
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
        GenericExpressionFactory.getBaseBuilder[RST_Merge](1, exprConfig)
    }

}
