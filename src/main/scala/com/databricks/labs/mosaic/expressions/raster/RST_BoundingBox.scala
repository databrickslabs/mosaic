package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.types.model.{GeometryTypeEnum, MosaicRasterTile}
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types._

/** Returns the world coordinates of the raster (x,y) pixel. */
case class RST_BoundingBox(
    raster: Expression,
    expressionConfig: MosaicExpressionConfig
) extends RasterExpression[RST_BoundingBox](raster, BinaryType, returnsRaster = false, expressionConfig = expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    /**
      * The function to be overridden by the extending class. It is called when
      * the expression is evaluated. It provides the raster to the expression.
      * It abstracts spark serialization from the caller.
      *
      * @param tile
      *   The raster tile to be used.
      * @return
      *   The result of the expression.
      */
    override def rasterTransform(tile: MosaicRasterTile): Any = {
        val raster = tile.raster
        val gt = raster.getRaster.GetGeoTransform()
        val (originX, originY) = rasterAPI.toWorldCoord(gt, 0, 0)
        val (endX, endY) = rasterAPI.toWorldCoord(gt, raster.xSize, raster.ySize)
        val geometryAPI = GeometryAPI(expressionConfig.getGeometryAPI)
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
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_BoundingBox extends WithExpressionInfo {

    override def name: String = "rst_boundingbox"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns the bounding box of the raster.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, b, c);
          |        (11.2, 12.3)
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_BoundingBox](1, expressionConfig)
    }

}
