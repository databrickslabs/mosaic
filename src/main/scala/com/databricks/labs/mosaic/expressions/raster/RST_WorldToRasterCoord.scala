package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.Raster2ArgExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder

/** Returns the world coordinate of the raster. */
case class RST_WorldToRasterCoord(
    path: Expression,
    x: Expression,
    y: Expression,
    expressionConfig: MosaicExpressionConfig
) extends Raster2ArgExpression[RST_WorldToRasterCoord](path, x, y, PixelCoordsType, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    /**
      * Returns the x and y of the raster by applying GeoTransform as a tuple of
      * Integers. This will ensure projection of the raster is respected.
      */
    override def rasterTransform(raster: MosaicRaster, arg1: Any, arg2: Any): Any = {
        val xGeo = arg1.asInstanceOf[Double]
        val yGeo = arg2.asInstanceOf[Double]
        val gt = raster.getRaster.GetGeoTransform()

        val (x, y) = rasterAPI.fromWorldCoord(gt, xGeo, yGeo)

        InternalRow.fromSeq(Seq(x, y))
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_WorldToRasterCoord extends WithExpressionInfo {

    override def name: String = "rst_worldtorastercoord"

    override def usage: String = "_FUNC_(expr1) - Returns x and y coordinates (pixel, line) of the pixel."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |       (11, 12)
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_WorldToRasterCoord](3, expressionConfig)
    }

}
