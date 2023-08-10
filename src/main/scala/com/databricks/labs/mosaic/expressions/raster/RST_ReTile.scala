package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.gdal_raster.RasterCleaner
import com.databricks.labs.mosaic.core.raster.operator.retile.ReTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterGeneratorExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}

/**
  * Returns a set of new rasters with the specified tile size (tileWidth x
  * tileHeight).
  */
case class RST_ReTile(
    rasterExpr: Expression,
    tileWidthExpr: Expression,
    tileHeightExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends RasterGeneratorExpression[RST_ReTile](rasterExpr, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    /**
      * Returns a set of new rasters with the specified tile size (tileWidth x
      * tileHeight).
      */
    override def rasterGenerator(raster: MosaicRaster): Seq[MosaicRaster] = {
        val tileWidthValue = tileWidthExpr.eval().asInstanceOf[Int]
        val tileHeightValue = tileHeightExpr.eval().asInstanceOf[Int]
        val result = ReTile.reTile(raster, tileWidthValue, tileHeightValue, geometryAPI, rasterAPI)
        RasterCleaner.dispose(raster)
        result
    }

    override def children: Seq[Expression] = Seq(rasterExpr, tileWidthExpr, tileHeightExpr)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_ReTile extends WithExpressionInfo {

    override def name: String = "rst_retile"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns a set of new rasters with the specified tile size (tileWidth x tileHeight).
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
        GenericExpressionFactory.getBaseBuilder[RST_ReTile](3, expressionConfig)
    }

}
