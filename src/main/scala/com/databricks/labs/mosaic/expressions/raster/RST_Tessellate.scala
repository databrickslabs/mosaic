package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.operator.retile.RasterTessellate
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterTessellateGeneratorExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, NullIntolerant}
import org.apache.spark.sql.types.{BooleanType, IntegerType}

/**
  * Returns a set of new rasters which are the result of the tessellation of the
  * input tile.
  */
case class RST_Tessellate(
                             rasterExpr: Expression,
                             resolutionExpr: Expression,
                             skipProjectExpr: Expression,
                             exprConfig: ExprConfig
) extends RasterTessellateGeneratorExpression[RST_Tessellate](rasterExpr, resolutionExpr, skipProjectExpr, exprConfig)
      with NullIntolerant
      with CodegenFallback {

    /**
      * Returns a set of new rasters which are the result of the tessellation of
      * the input tile.
      */
    override def rasterGenerator(tile: RasterTile, resolution: Int, skipProject: Boolean): Seq[RasterTile] = {
        RasterTessellate.tessellate(
            tile.raster,
            resolution,
            skipProject,
            indexSystem,
            geometryAPI,
            Option(exprConfig)
        )
    }

    override def children: Seq[Expression] = Seq(rasterExpr, resolutionExpr, skipProjectExpr)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Tessellate extends WithExpressionInfo {

    override def name: String = "rst_tessellate"

    override def usage: String =
        """
          |_FUNC_(expr1, expr2) - Returns a set of new tile tiles with the specified resolution within configured grid.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile, 3);
          |        {index_id, raster_tile, tile_width, tile_height}
          |        {index_id, raster_tile, tile_width, tile_height}
          |        ...
          |  """.stripMargin

    // added `skipProject` optional column
//    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
//        GenericExpressionFactory.getBaseBuilder[RST_Tessellate](2, exprConfig)
//    }

    override def builder(exprConfig: ExprConfig): FunctionBuilder = { (children: Seq[Expression]) =>
    {
        val skipExpr = if (children.length < 3) new Literal(false, BooleanType) else children(2)
        RST_Tessellate(children.head, children(1), skipExpr, exprConfig)
    }
    }

}
