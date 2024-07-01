package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.Raster2ArgExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types._

/** Returns the world coordinates of the raster (x,y) pixel. */
case class RST_RasterToWorldCoord(
                                     raster: Expression,
                                     x: Expression,
                                     y: Expression,
                                     exprConfig: ExprConfig
) extends Raster2ArgExpression[RST_RasterToWorldCoord](raster, x, y, returnsRaster = false, exprConfig = exprConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = StringType

    /**
      * Returns the world coordinates of the raster (x,y) pixel by applying
      * GeoTransform. This ensures the projection of the raster is respected.
      * The output is a WKT point.
      */
    override def rasterTransform(tile: RasterTile, arg1: Any, arg2: Any): Any = {
        val x = arg1.asInstanceOf[Int]
        val y = arg2.asInstanceOf[Int]
        val geometryAPI = GeometryAPI(exprConfig.getGeometryAPI)
        val point: MosaicPoint = tile.raster.getGeoTransformOpt match {
            case Some(gt) =>
                val (xGeo, yGeo) = GDAL.toWorldCoord(gt, x, y)
                geometryAPI.fromCoords(Seq(xGeo, yGeo))
            case _ => geometryAPI.fromCoords(Seq(0d, 0d)) // "empty" Point
        }
        geometryAPI.serialize(point, StringType)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_RasterToWorldCoord extends WithExpressionInfo {

    override def name: String = "rst_rastertoworldcoord"

    override def usage: String =
        """
          |_FUNC_(expr1, expr2, expr3) - Returns the (x, y) pixel in world coordinates using geo transform of the raster tile.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile, x, y);
          |        (11.2, 12.3)
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_RasterToWorldCoord](3, exprConfig)
    }

}
