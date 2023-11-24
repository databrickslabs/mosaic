package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.operator.separate.SeparateBands
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterGeneratorExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}

/**
  * Returns a set of new single-band rasters, one for each band in the input raster.
  */
case class RST_SeparateBands(
    rasterExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends RasterGeneratorExpression[RST_SeparateBands](rasterExpr, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    /**
      * Returns a set of new single-band rasters, one for each band in the input raster.
      */
    override def rasterGenerator(tile: MosaicRasterTile): Seq[MosaicRasterTile] = {
        SeparateBands.separate(tile)
    }

    override def children: Seq[Expression] = Seq(rasterExpr)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_SeparateBands extends WithExpressionInfo {

    override def name: String = "rst_separatebands"

    override def usage: String =
        """
          |_FUNC_(expr1) - Separates raster bands into separate rasters. Empty bands are discarded.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |        {index_id, raster_tile, parentPath, driver}
          |        {index_id, raster_tile, parentPath, driver}
          |        {index_id, raster_tile, parentPath, driver}
          |        ...
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_SeparateBands](3, expressionConfig)
    }

}
