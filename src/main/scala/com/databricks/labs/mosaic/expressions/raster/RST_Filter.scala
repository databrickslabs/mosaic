package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.Raster2ArgExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.unsafe.types.UTF8String

/** The expression for applying NxN filter on a raster. */
case class RST_Filter(
    rastersExpr: Expression,
    kernelSizeExpr: Expression,
    operationExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends Raster2ArgExpression[RST_Filter](
      rastersExpr,
      kernelSizeExpr,
      operationExpr,
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
    override def rasterTransform(tile: MosaicRasterTile, arg1: Any, arg2: Any): Any = {
        val n = arg1.asInstanceOf[Int]
        val operation = arg2.asInstanceOf[UTF8String].toString
        tile.copy(
          raster = tile.getRaster.filter(n, operation)
        )
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Filter extends WithExpressionInfo {

    override def name: String = "rst_filter"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns a raster with the filter applied.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster, kernelSize, operation);
          |        {index_id, clipped_raster, parentPath, driver}
          |        {index_id, clipped_raster, parentPath, driver}
          |        ...
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Filter](3, expressionConfig)
    }

}
