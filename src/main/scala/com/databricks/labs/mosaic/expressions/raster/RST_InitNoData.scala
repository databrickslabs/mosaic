package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALWarp
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import com.databricks.labs.mosaic.utils.PathUtils
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types.DataType

/** The expression that initializes no data values of a raster. */
case class RST_InitNoData(
    tileExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends RasterExpression[RST_InitNoData](
      tileExpr,
      returnsRaster = true,
      expressionConfig = expressionConfig
    )
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = RasterTileType(expressionConfig.getCellIdType, tileExpr)

    /**
      * Initializes no data values of a raster.
      *
      * @param tile
      *   The raster to be used.
      * @return
      *   The raster with initialized no data values.
      */
    override def rasterTransform(tile: MosaicRasterTile): Any = {
        val noDataValues = tile.getRaster.getBands.map(_.noDataValue).mkString(" ")
        val dstNoDataValues = tile.getRaster.getBands
            .map(_.getBand.getDataType)
            .map(GDAL.getNoDataConstant)
            .mkString(" ")
        val resultPath = PathUtils.createTmpFilePath(GDAL.getExtension(tile.getDriver))
        val cmd = s"""gdalwarp -of ${tile.getDriver} -dstnodata "$dstNoDataValues" -srcnodata "$noDataValues""""
        tile.copy(
          raster = GDALWarp.executeWarp(
            resultPath,
            Seq(tile.getRaster),
            command = cmd
          )
        )
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_InitNoData extends WithExpressionInfo {

    override def name: String = "rst_initnodata"

    override def usage: String =
        """
          |_FUNC_(expr1) - Initializes the nodata value of the raster bands.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |        {index_id, clipped_raster, parentPath, driver}
          |        {index_id, clipped_raster, parentPath, driver}
          |        ...
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_InitNoData](1, expressionConfig)
    }

}
