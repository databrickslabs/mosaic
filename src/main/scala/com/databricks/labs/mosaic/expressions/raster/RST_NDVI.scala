package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.operator.NDVI
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.Raster2ArgExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types.DataType

/** The expression for computing NDVI index. */
case class RST_NDVI(
                       tileExpr: Expression,
                       redIndex: Expression,
                       nirIndex: Expression,
                       exprConfig: ExprConfig
) extends Raster2ArgExpression[RST_NDVI](
      tileExpr,
      redIndex,
      nirIndex,
      returnsRaster = true,
      exprConfig = exprConfig
    )
      with NullIntolerant
      with CodegenFallback {

    // serialize data type
    override def dataType: DataType = {
        RasterTileType(exprConfig.getCellIdType, tileExpr, exprConfig.isRasterUseCheckpoint)
    }

    /**
      * Computes NDVI index.
      * @param tile
      *   The raster to be used.
      * @param arg1
      *   The red band index.
      * @param arg2
      *   The nir band index.
      * @return
      *   The raster contains NDVI index.
      */
    override def rasterTransform(tile: RasterTile, arg1: Any, arg2: Any): Any = {
        val redInd = arg1.asInstanceOf[Int]
        val nirInd = arg2.asInstanceOf[Int]
        tile.copy(raster = NDVI.compute(tile.raster, redInd, nirInd, Option(exprConfig)))
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_NDVI extends WithExpressionInfo {

    override def name: String = "rst_ndvi"

    override def usage: String =
        """
          |_FUNC_(expr1, expr2, expr3) - NDVI index computed by raster tile red_index and nir_index bands.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile, 1, 2);
          |        {index_id, raster, parent_path, driver}
          |        ...
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_NDVI](3, exprConfig)
    }

}
