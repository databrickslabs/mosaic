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
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType

/** Returns a raster with the specified no data values. */
case class RST_SetNoData(
    tileExpr: Expression,
    noDataExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends Raster1ArgExpression[RST_SetNoData](
      tileExpr,
      noDataExpr,
      returnsRaster = true,
      expressionConfig = expressionConfig
    )
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = RasterTileType(expressionConfig.getCellIdType, tileExpr)

    /**
      * Returns a raster with the specified no data values.
      * @param tile
      *   The input raster tile.
      * @param arg1
      *   The no data values.
      * @return
      *   The raster with the specified no data values.
      */
    override def rasterTransform(tile: MosaicRasterTile, arg1: Any): Any = {
        val noDataValues = tile.getRaster.getBands.map(_.noDataValue).mkString(" ")
        val dstNoDataValues = (arg1 match {
            case d: Double            => Array.fill[Double](tile.getRaster.numBands)(d)
            case i: Int               => Array.fill[Double](tile.getRaster.numBands)(i.toDouble)
            case l: Long              => Array.fill[Double](tile.getRaster.numBands)(l.toDouble)
            case arrayData: ArrayData => arrayData.array.map(_.toString.toDouble) // Trick to convert SQL decimal to double
            case _ => throw new IllegalArgumentException("No data values must be an array of numerical or a numerical value.")
        }).mkString(" ")
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
object RST_SetNoData extends WithExpressionInfo {

    override def name: String = "rst_setnodata"

    override def usage: String =
        """
          |_FUNC_(expr1, expr2) - Sets the nodata value of the raster tile for all bands.
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
