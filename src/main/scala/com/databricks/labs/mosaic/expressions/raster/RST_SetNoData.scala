package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALWarp
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.Raster1ArgExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType

/** Returns a tile with the specified no data values. */
case class RST_SetNoData(
                            tileExpr: Expression,
                            noDataExpr: Expression,
                            exprConfig: ExprConfig
) extends Raster1ArgExpression[RST_SetNoData](
      tileExpr,
      noDataExpr,
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
      * Returns a tile with the specified no data values.
      * @param tile
      *   The input tile tile.
      * @param arg1
      *   The no data values.
      * @return
      *   The tile with the specified no data values.
      */
    override def rasterTransform(tile: RasterTile, arg1: Any): Any = {
        val raster = tile.raster
        val noDataValues = raster.getBands.map(_.noDataValue).mkString(" ")
        val dstNoDataValues = (arg1 match {
            case d: Double            => Array.fill[Double](raster.numBands)(d)
            case i: Int               => Array.fill[Double](raster.numBands)(i.toDouble)
            case l: Long              => Array.fill[Double](raster.numBands)(l.toDouble)
            case arrayData: ArrayData => arrayData.array.map(_.toString.toDouble) // Trick to convert SQL decimal to double
            case _ => throw new IllegalArgumentException("No data values must be an array of numerical or a numerical value.")
        }).mkString(" ")
        val resultPath = raster.createTmpFileFromDriver(Option(exprConfig))
        val cmd = s"""gdalwarp -of ${raster.getDriverName()} -dstnodata "$dstNoDataValues" -srcnodata "$noDataValues""""
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
object RST_SetNoData extends WithExpressionInfo {

    override def name: String = "rst_setnodata"

    override def usage: String =
        """
          |_FUNC_(expr1, expr2) - Sets the nodata value of the tile tile for all bands.
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

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_SetNoData](2, exprConfig)
    }

}
