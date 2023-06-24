package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.gdal_raster.RasterCleaner
import com.databricks.labs.mosaic.core.raster.operator.retile.RasterTessellate
import com.databricks.labs.mosaic.core.types.model.MosaicRasterChip
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterTessellateGeneratorExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}

/**
  * Returns a set of new rasters with the specified tile size (tileWidth x
  * tileHeight).
  */
case class RST_Tessellate(
    rasterExpr: Expression,
    resolutionExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends RasterTessellateGeneratorExpression[RST_Tessellate](rasterExpr, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    /**
      * Returns a set of new rasters with the specified tile size (tileWidth x
      * tileHeight).
      */
    override def rasterGenerator(raster: MosaicRaster): Seq[MosaicRasterChip] = {
        val resolution = resolutionExpr.eval().asInstanceOf[Int]
        val result = RasterTessellate.tessellate(
          raster,
          resolution,
          indexSystem,
          geometryAPI
        )
        RasterCleaner.dispose(raster)
        result
    }

    override def children: Seq[Expression] = Seq(rasterExpr, resolutionExpr)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Tessellate extends WithExpressionInfo {

    override def name: String = "rst_tessellate"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns a set of new rasters with the specified resolution within configured grid.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, b);
          |        /path/to/raster_tile_1.tif
          |        /path/to/raster_tile_2.tif
          |        /path/to/raster_tile_3.tif
          |        ...
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Tessellate](2, expressionConfig)
    }

}
