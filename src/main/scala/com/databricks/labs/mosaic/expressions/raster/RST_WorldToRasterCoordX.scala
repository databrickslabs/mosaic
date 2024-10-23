package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.Raster2ArgExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types.IntegerType

/** Returns the x coordinate of the raster. */
case class RST_WorldToRasterCoordX(
    raster: Expression,
    x: Expression,
    y: Expression,
    expressionConfig: MosaicExpressionConfig
) extends Raster2ArgExpression[RST_WorldToRasterCoordX](raster, x, y, returnsRaster = false, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: IntegerType = IntegerType

    /**
      * Returns the x coordinate of the raster by applying GeoTransform. This
      * will ensure projection of the raster is respected.
      */
    override def rasterTransform(tile: MosaicRasterTile, arg1: Any, arg2: Any): Any = {
        val xGeo = arg1.asInstanceOf[Double]
        val yGeo = arg2.asInstanceOf[Double]
        val gt = tile.getRaster.getRaster.GetGeoTransform()
        GDAL.fromWorldCoord(gt, xGeo, yGeo)._1
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_WorldToRasterCoordX extends WithExpressionInfo {

    override def name: String = "rst_worldtorastercoordx"

    override def usage: String = "_FUNC_(expr1, expr2, expr3) - Returns x coordinate (pixel, line) of the raster tile pixel coord."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile, 1.123, 1.123);
          |       11
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_WorldToRasterCoordX](3, expressionConfig)
    }

}
