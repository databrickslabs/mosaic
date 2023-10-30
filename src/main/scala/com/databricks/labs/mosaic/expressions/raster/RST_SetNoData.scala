package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALWarp
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.Raster1ArgExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import com.databricks.labs.mosaic.utils.PathUtils
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}

/** Returns a raster with the specified no data values. */
case class RST_SetNoData(
    rastersExpr: Expression,
    noDataExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends Raster1ArgExpression[RST_SetNoData](
      rastersExpr,
      noDataExpr,
      RasterTileType(expressionConfig.getCellIdType),
      returnsRaster = true,
      expressionConfig = expressionConfig
    )
      with NullIntolerant
      with CodegenFallback {

    /**
      * Returns a raster with the specified no data values.
      * @param tile
      *   The input raster tile.
      * @param arg1
      *   The no data values.
      * @return
      *   The raster with the specified no data values.
      */
    override def rasterTransform(tile: => MosaicRasterTile, arg1: Any): Any = {
        val noDataValues = tile.getRaster.getBands.map(_.noDataValue).mkString(" ")
        val dstNoDataValues = (arg1 match {
            case doubles: Array[Double] => doubles
            case d: Double              => Array.fill[Double](tile.getRaster.numBands)(d)
            case _                      => throw new IllegalArgumentException("No data values must be an array of doubles or a double")
        }).mkString(" ")
        val resultPath = PathUtils.createTmpFilePath(tile.getRaster.uuid.toString, GDAL.getExtension(tile.getDriver))
        val result = GDALWarp.executeWarp(
          resultPath,
          isTemp = true,
          Seq(tile.getRaster),
          command = s"""gdalwarp -of ${tile.getDriver} -dstnodata "$dstNoDataValues" -srcnodata "$noDataValues""""
        )
        new MosaicRasterTile(
          tile.getIndex,
          result.flushCache(),
          tile.getParentPath,
          tile.getDriver
        )
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_SetNoData extends WithExpressionInfo {

    override def name: String = "rst_set_no_data"

    override def usage: String =
        """
          |_FUNC_(expr1, expr2) - Returns a raster clipped by provided vector.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile, 0.0);
          |        {index_id, clipped_raster, parentPath, driver}
          |        {index_id, clipped_raster, parentPath, driver}
          |        ...
          |      > SELECT _FUNC_(raster_tile, array(0.0, 0.0, 0.0));
          |        {index_id, clipped_raster, parentPath, driver}
          |        {index_id, clipped_raster, parentPath, driver}
          |        ...
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_SetNoData](2, expressionConfig)
    }

}
