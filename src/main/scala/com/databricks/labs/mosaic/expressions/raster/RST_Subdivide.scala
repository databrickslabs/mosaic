package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.operator.retile.BalancedSubdivision
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterGeneratorExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}

/** Returns a set of new rasters with the specified tile size (In MB). */
case class RST_Subdivide(
    rasterExpr: Expression,
    sizeInMB: Expression,
    expressionConfig: MosaicExpressionConfig
) extends RasterGeneratorExpression[RST_Subdivide](rasterExpr, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    /** Returns a set of new rasters with the specified tile size (In MB). */
    override def rasterGenerator(tile: MosaicRasterTile): Seq[MosaicRasterTile] = {
        val targetSize = sizeInMB.eval().asInstanceOf[Int]
        BalancedSubdivision.splitRaster(tile, targetSize)
    }

    override def children: Seq[Expression] = Seq(rasterExpr, sizeInMB)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Subdivide extends WithExpressionInfo {

    override def name: String = "rst_subdivide"

    override def usage: String =
        """
          |_FUNC_(expr1, expr2) - Returns a set of new raster tiles with same aspect ratio that are not larger than the
          |                       threshold memory footprint in MBs.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile, 32);
          |        {index_id, raster_tile, tile_width, tile_height}
          |        {index_id, raster_tile, tile_width, tile_height}
          |        ...
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Subdivide](2, expressionConfig)
    }

}
