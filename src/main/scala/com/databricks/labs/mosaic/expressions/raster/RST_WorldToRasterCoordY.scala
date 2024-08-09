package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.Raster2ArgExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types.IntegerType

/** Returns the Y coordinate of the tile. */
case class RST_WorldToRasterCoordY(
                                      raster: Expression,
                                      x: Expression,
                                      y: Expression,
                                      exprConfig: ExprConfig
) extends Raster2ArgExpression[RST_WorldToRasterCoordY](raster, x, y, returnsRaster = false, exprConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: IntegerType = IntegerType

    /**
      * Returns the y coordinate of the tile by applying GeoTransform. This
      * will ensure projection of the tile is respected, default 0.
      */
    override def rasterTransform(tile: RasterTile, arg1: Any, arg2: Any): Any = {
        val xGeo = arg1.asInstanceOf[Double]
        val yGeo = arg2.asInstanceOf[Double]
        tile.raster.getGeoTransformOpt match {
            case Some(gt) => GDAL.fromWorldCoord(gt, xGeo, yGeo)._2
            case _ => 0
        }
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_WorldToRasterCoordY extends WithExpressionInfo {

    override def name: String = "rst_worldtorastercoordy"

    override def usage: String = "_FUNC_(expr1, expr2, expr3) - Returns y coordinate (pixel, line) of the tile tile pixel coord."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile, 1.123, 1.123);
          |       12
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_WorldToRasterCoordY](3, exprConfig)
    }

}
