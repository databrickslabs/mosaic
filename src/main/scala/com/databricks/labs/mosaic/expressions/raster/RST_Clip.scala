package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.raster.operator.clip.RasterClipByVector
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.Raster1ArgExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}

/**
  * Returns a set of new rasters with the specified tile size (tileWidth x
  * tileHeight).
  */
case class RST_Clip(
    rastersExpr: Expression,
    geometryExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends Raster1ArgExpression[RST_Clip](
      rastersExpr,
      geometryExpr,
      RasterTileType(expressionConfig.getCellIdType),
      returnsRaster = true,
      expressionConfig = expressionConfig
    )
      with NullIntolerant
      with CodegenFallback {

    val geometryAPI: GeometryAPI = GeometryAPI(expressionConfig.getGeometryAPI)

    /**
      * The function to be overridden by the extending class. It is called when
      * the expression is evaluated. It provides the raster and the arguments to
      * the expression. It abstracts spark serialization from the caller.
      *
      * @param tile
      *   The raster to be used.
      * @param arg1
      *   The first argument.
      * @return
      *   A result of the expression.
      */
    override def rasterTransform(tile: MosaicRasterTile, arg1: Any): Any = {
        val geometry = geometryAPI.geometry(arg1, geometryExpr.dataType)
        val geomCRS = geometry.getSpatialReferenceOSR
        MosaicRasterTile(
          tile.index,
          RasterClipByVector.clip(tile.raster, geometry, geomCRS, geometryAPI),
          tile.parentPath,
          tile.driver
        )
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Clip extends WithExpressionInfo {

    override def name: String = "rst_clip"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns a raster clipped by provided vector.
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
        GenericExpressionFactory.getBaseBuilder[RST_Clip](2, expressionConfig)
    }

}
