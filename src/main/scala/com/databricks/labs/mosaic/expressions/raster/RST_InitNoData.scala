package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.io.RasterIO.createTmpFileFromDriver
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALWarp
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types.DataType

/** The expression that initializes no data values of a tile. */
case class RST_InitNoData(
                             tileExpr: Expression,
                             exprConfig: ExprConfig
) extends RasterExpression[RST_InitNoData](
      tileExpr,
      returnsRaster = true,
      exprConfig = exprConfig
    )
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = {
        RasterTileType(exprConfig.getCellIdType, tileExpr, exprConfig.isRasterUseCheckpoint)
    }

    /**
      * Initializes no data values of a tile.
      *
      * @param tile
      *   The tile to be used.
      * @return
      *   The tile with initialized no data values.
      */
    override def rasterTransform(tile: RasterTile): Any = {
        val raster = tile.raster
        val noDataValues = raster.getBands.map(_.noDataValue).mkString(" ")
        val dstNoDataValues = raster.getBands
            .map(_.getBand.getDataType)
            .map(GDAL.getNoDataConstant)
            .mkString(" ")
        val driverShortName = raster.getDriverName()
        val resultPath = createTmpFileFromDriver(driverShortName, Option(exprConfig))
        val cmd = s"""gdalwarp -of ${driverShortName} -dstnodata "$dstNoDataValues" -srcnodata "$noDataValues""""
        tile.copy(
            raster = GDALWarp.executeWarp(
                resultPath,
                Seq(raster),
                command = cmd,
                Option(exprConfig)
            )
        )
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_InitNoData extends WithExpressionInfo {

    override def name: String = "rst_initnodata"

    override def usage: String =
        """
          |_FUNC_(expr1) - Initializes the nodata value of the tile bands.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |        {index_id, clipped_raster, parentPath, driver}
          |        {index_id, clipped_raster, parentPath, driver}
          |        ...
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_InitNoData](1, exprConfig)
    }

}
