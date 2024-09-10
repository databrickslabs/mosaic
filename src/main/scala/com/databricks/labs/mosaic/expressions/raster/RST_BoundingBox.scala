package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.POLYGON_EMPTY_WKT
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.types.model.{GeometryTypeEnum, RasterTile}
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types._

import scala.util.Try

/** The expression for extracting the bounding box of a tile. */
case class RST_BoundingBox(
                              raster: Expression,
                              exprConfig: ExprConfig
) extends RasterExpression[RST_BoundingBox](raster, returnsRaster = false, exprConfig = exprConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = BinaryType

    /**
      * Computes the bounding box of the tile. The bbox is returned as a WKB
      * polygon.
      *
      * @param tile
      *   The tile tile to be used.
      * @return
      *   The bounding box of the tile as a WKB polygon.
      */
    override def rasterTransform(tile: RasterTile): Any = Try {
        val raster = tile.raster
        val gt = raster.getGeoTransformOpt.orNull
        val (originX, originY) = GDAL.toWorldCoord(gt, 0, 0)
        val (endX, endY) = GDAL.toWorldCoord(gt, raster.xSize, raster.ySize)
        val geometryAPI = GeometryAPI(exprConfig.getGeometryAPI)
        val bboxPolygon = geometryAPI.geometry(
          Seq(
            Seq(originX, originY),
            Seq(originX, endY),
            Seq(endX, endY),
            Seq(endX, originY),
            Seq(originX, originY)
          ).map(geometryAPI.fromCoords),
          GeometryTypeEnum.POLYGON
        )

        bboxPolygon.toWKB
    }.getOrElse(GeometryAPI(exprConfig.getGeometryAPI).geometry(POLYGON_EMPTY_WKT, "WKT").toWKB)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_BoundingBox extends WithExpressionInfo {

    override def name: String = "rst_boundingbox"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns the bounding box of the tile tile.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |        POLYGON ((-180 -90, -180 90, 180 90, 180 -90, -180 -90))
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_BoundingBox](1, exprConfig)
    }

}
