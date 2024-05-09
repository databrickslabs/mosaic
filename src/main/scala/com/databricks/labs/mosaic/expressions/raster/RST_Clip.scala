package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.operator.clip.RasterClipByVector
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.Raster1ArgExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}

/** The expression for clipping a raster by a vector. */
case class RST_Clip(
    rastersExpr: Expression,
    geometryExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends Raster1ArgExpression[RST_Clip](
      rastersExpr,
      geometryExpr,
      returnsRaster = true,
      expressionConfig = expressionConfig
    )
      with NullIntolerant
      with CodegenFallback {

    override def dataType: org.apache.spark.sql.types.DataType = {
        GDAL.enable(expressionConfig)
        RasterTileType(expressionConfig.getCellIdType, rastersExpr, expressionConfig.isRasterUseCheckpoint)
    }

    val geometryAPI: GeometryAPI = GeometryAPI(expressionConfig.getGeometryAPI)

    /**
      * Clips a raster by a vector.
      *
      * @param tile
      *   The raster to be used.
      * @param arg1
      *   The vector to be used.
      * @return
      *   The clipped raster.
      */
    override def rasterTransform(tile: MosaicRasterTile, arg1: Any): Any = {
        val geometry = geometryAPI.geometry(arg1, geometryExpr.dataType)
        val geomCRS = geometry.getSpatialReferenceOSR
        tile.copy(
          raster = RasterClipByVector.clip(tile.getRaster, geometry, geomCRS, geometryAPI)
        )
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Clip extends WithExpressionInfo {

    override def name: String = "rst_clip"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns a raster tile clipped by provided vector.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile, vector);
          |        {index_id, clipped_raster, parentPath, driver}
          |        {index_id, clipped_raster, parentPath, driver}
          |        ...
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Clip](2, expressionConfig)
    }

}
