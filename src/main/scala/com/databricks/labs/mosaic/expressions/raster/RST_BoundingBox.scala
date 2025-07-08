package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types._

/** The expression for extracting the bounding box of a raster. */
case class RST_BoundingBox(
    raster: Expression,
    expressionConfig: MosaicExpressionConfig
) extends RasterExpression[RST_BoundingBox](raster, returnsRaster = false, expressionConfig = expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = BinaryType

    /**
      * Computes the bounding box of the raster. The bbox is returned as a WKB
      * polygon.
      *
      * @param tile
      *   The raster tile to be used.
      * @return
      *   The bounding box of the raster as a WKB polygon.
      */
    override def rasterTransform(tile: MosaicRasterTile): Any = {
        var raster = tile.getRaster
        val geometryAPI = GeometryAPI(expressionConfig.getGeometryAPI)
        val bbox = raster.bbox(geometryAPI, raster.getSpatialReference)
        raster = null
        bbox.toWKB
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_BoundingBox extends WithExpressionInfo {

    override def name: String = "rst_boundingbox"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns the bounding box of the raster tile.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |        POLYGON ((-180 -90, -180 90, 180 90, 180 -90, -180 -90))
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_BoundingBox](1, expressionConfig)
    }

}
