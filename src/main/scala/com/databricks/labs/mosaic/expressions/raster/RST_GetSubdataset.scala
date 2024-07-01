package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.Raster1ArgExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

/** Returns the subdatasets of the raster. */
case class RST_GetSubdataset(
                                tileExpr: Expression,
                                subsetName: Expression,
                                exprConfig: ExprConfig
) extends Raster1ArgExpression[RST_GetSubdataset](
      tileExpr,
      subsetName,
      returnsRaster = true,
      exprConfig
    )
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = {
        RasterTileType(exprConfig.getCellIdType, tileExpr, exprConfig.isRasterUseCheckpoint)
    }

    /** Returns the subdatasets of the raster. */
    override def rasterTransform(tile: RasterTile, arg1: Any): Any = {
        val subsetName = arg1.asInstanceOf[UTF8String].toString
        tile.copy(raster = tile.raster.getSubdataset(subsetName))
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_GetSubdataset extends WithExpressionInfo {

    override def name: String = "rst_getsubdataset"

    override def usage: String = "_FUNC_(expr1, expr2) - Extracts subdataset raster tile."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile, 'SUBDATASET_1_NAME');
          |        {index_id, raster, parent_path, driver}
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_GetSubdataset](2, exprConfig)
    }

}
