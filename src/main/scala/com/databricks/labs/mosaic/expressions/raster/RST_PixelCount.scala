package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.Raster2ArgExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

import scala.util.Try

/** Returns an array containing valid pixel count values for each band. */
case class RST_PixelCount(
                             rasterExpr: Expression,
                             noDataExpr: Expression,
                             allExpr: Expression,
                             exprConfig: ExprConfig)
    extends Raster2ArgExpression[RST_PixelCount](rasterExpr, noDataExpr, allExpr, returnsRaster = false, exprConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = ArrayType(LongType)

    /**
      * Returns an array containing valid pixel count values for each band.
      * - default is to exclude nodata and mask pixels.
      * - if countNoData specified as true, include the noData (not mask) pixels in the count (default is false).
      * - if countAll specified as true, simply return bandX * bandY in the count (default is false). countAll ignores
      *   countNodData
      */
    override def rasterTransform(tile: RasterTile, arg1: Any, arg2: Any): Any =
        Try{
            val countNoData = arg1.asInstanceOf[Boolean]
            val countAll = arg2.asInstanceOf[Boolean]
            val raster = tile.raster
            val nBands = raster.getDatasetOrNull().GetRasterCount()
            val values = (1 to nBands).map (
                raster.getBand (_).pixelCount (countNoData, countAll) // <- pixelCount
            )
            ArrayData.toArrayData(values.toArray)
        }.getOrElse(ArrayData.toArrayData(Array.empty[Int]))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_PixelCount extends WithExpressionInfo {

    override def name: String = "rst_pixelcount"

    override def usage: String = "_FUNC_(expr1) - Returns an array containing pixel count values for each band (default excludes nodata and mask)."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |       [12, 212, 313]
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_PixelCount](3, exprConfig)
    }

}
