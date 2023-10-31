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

/** The expression that initializes no data values of a raster. */
case class RST_InitNoData(
    rastersExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends RasterExpression[RST_InitNoData](
      rastersExpr,
      RasterTileType(expressionConfig.getCellIdType),
      returnsRaster = true,
      expressionConfig = expressionConfig
    )
      with NullIntolerant
      with CodegenFallback {

    /**
      * Initializes no data values of a raster.
      *
      * @param tile
      *   The raster to be used.
      * @return
      *   The raster with initialized no data values.
      */
    override def rasterTransform(tile: => MosaicRasterTile): Any = {
        val noDataValues = tile.getRaster.getBands.map(_.noDataValue).mkString(" ")
        val dstNoDataValues = tile.getRaster.getBands
            .map(_.getBand.getDataType)
            .map(GDAL.getNoDataConstant)
            .mkString(" ")
        val resultPath = PathUtils.createTmpFilePath(tile.getRaster.uuid.toString, GDAL.getExtension(tile.getDriver))
        val result = GDALWarp.executeWarp(
          resultPath,
          isTemp = true,
          Seq(tile.getRaster),
          command = s"""gdalwarp -of ${tile.getDriver} -dstnodata "$dstNoDataValues" -srcnodata "$noDataValues""""
        )
        new MosaicRasterTile(
          tile.getIndex,
          result,
          tile.getParentPath,
          tile.getDriver
        )
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_InitNoData extends WithExpressionInfo {

    override def name: String = "rst_init_no_data"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns a raster clipped by provided vector.
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
