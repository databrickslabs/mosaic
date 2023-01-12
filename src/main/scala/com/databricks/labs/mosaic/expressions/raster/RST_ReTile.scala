package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterGeneratorExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.hive.common.util.Murmur3
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback

/**
  * Returns a set of new rasters with the specified tile size (tileWidth x
  * tileHeight).
  */
case class RST_ReTile(
    pathExpr: Expression,
    tileWidthExpr: Expression,
    tileHeightExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends RasterGeneratorExpression[RST_ReTile](pathExpr, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    /**
      * Returns a set of new rasters with the specified tile size (tileWidth x
      * tileHeight).
      */
    override def rasterGenerator(raster: MosaicRaster): Seq[(Long, (Int, Int, Int, Int))] = {
        val tileWidthValue = tileWidthExpr.eval().asInstanceOf[Int]
        val tileHeightValue = tileHeightExpr.eval().asInstanceOf[Int]

        val xSize = raster.xSize
        val ySize = raster.ySize

        val xTiles = Math.ceil(xSize / tileWidthValue).toInt
        val yTiles = Math.ceil(ySize / tileHeightValue).toInt

        val tiles = for (x <- 0 until xTiles; y <- 0 until yTiles) yield {
            val xMin = x * tileWidthValue
            val yMin = y * tileHeightValue
            val xMax = Math.min(xMin + tileWidthValue, xSize)
            val yMax = Math.min(yMin + tileHeightValue, ySize)
            val id = Murmur3.hash64(s"${raster.toString}$xMin$yMin$xMax$yMax".getBytes)
            (id, (xMin, yMin, xMax, yMax))
        }

        tiles
    }

    override def children: Seq[Expression] = Seq(pathExpr, tileWidthExpr, tileHeightExpr)

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
