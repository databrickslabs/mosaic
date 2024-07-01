package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.operator.retile.RasterTessellate
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterTessellateGeneratorExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}

/**
  * Returns a set of new rasters which are the result of the tessellation of the
  * input raster.
  */
case class RST_Tessellate(
                             rasterExpr: Expression,
                             resolutionExpr: Expression,
                             exprConfig: ExprConfig
) extends RasterTessellateGeneratorExpression[RST_Tessellate](rasterExpr, resolutionExpr, exprConfig)
      with NullIntolerant
      with CodegenFallback {

    /**
      * Returns a set of new rasters which are the result of the tessellation of
      * the input raster.
      */
    override def rasterGenerator(tile: RasterTile, resolution: Int): Seq[RasterTile] = {
        RasterTessellate.tessellate(
            tile.raster,
            resolution,
            indexSystem,
            geometryAPI,
            Option(exprConfig)
        )
    }

    override def children: Seq[Expression] = Seq(rasterExpr, resolutionExpr)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Tessellate extends WithExpressionInfo {

    override def name: String = "rst_tessellate"

    override def usage: String =
        """
          |_FUNC_(expr1, expr2) - Returns a set of new raster tiles with the specified resolution within configured grid.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile, 3);
          |        {index_id, raster_tile, tile_width, tile_height}
          |        {index_id, raster_tile, tile_width, tile_height}
          |        ...
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Tessellate](2, exprConfig)
    }

}
