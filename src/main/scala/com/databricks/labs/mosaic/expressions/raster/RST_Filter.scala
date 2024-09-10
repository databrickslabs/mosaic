package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.Raster2ArgExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

/** The expression for applying NxN filter on a tile. */
case class RST_Filter(
                         rastersExpr: Expression,
                         kernelSizeExpr: Expression,
                         operationExpr: Expression,
                         exprConfig: ExprConfig
) extends Raster2ArgExpression[RST_Filter](
      rastersExpr,
      kernelSizeExpr,
      operationExpr,
      returnsRaster = true,
      exprConfig = exprConfig
    )
      with NullIntolerant
      with CodegenFallback {

    // serialize data type
    override def dataType: DataType = {
        RasterTileType(exprConfig.getCellIdType, rastersExpr, exprConfig.isRasterUseCheckpoint)
    }

    val geometryAPI: GeometryAPI = GeometryAPI(exprConfig.getGeometryAPI)

    /**
      * Clips a tile by a vector.
      *
      * @param tile
      *   The tile to be used.
      * @param arg1
      *   The vector to be used.
      * @return
      *   The clipped tile.
      */
    override def rasterTransform(tile: RasterTile, arg1: Any, arg2: Any): Any = {
        val n = arg1.asInstanceOf[Int]
        val operation = arg2.asInstanceOf[UTF8String].toString
        tile.copy(
          raster = tile.raster.filter(n, operation)
        )
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Filter extends WithExpressionInfo {

    override def name: String = "rst_filter"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns a tile with the filter applied.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(tile, kernelSize, operation);
          |        {index_id, clipped_raster, parentPath, driver}
          |        {index_id, clipped_raster, parentPath, driver}
          |        ...
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Filter](3, exprConfig)
    }

}
