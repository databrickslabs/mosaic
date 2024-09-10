package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.io.RasterIO.createTmpFileFromDriver
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALWarp
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

import scala.util.Try

/** Returns the median value per band of the tile. */
case class RST_Median(rasterExpr: Expression, exprConfig: ExprConfig)
    extends RasterExpression[RST_Median](rasterExpr, returnsRaster = false, exprConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = ArrayType(DoubleType)

    /** Returns the median value per band of the tile. */
    override def rasterTransform(tile: RasterTile): Any =
        Try {
            val raster = tile.raster
            val width = raster.xSize * raster.pixelXSize
            val height = raster.ySize * raster.pixelYSize
            val driverShortName = raster.getDriverName()
            val resultFileName = createTmpFileFromDriver(driverShortName, Option(exprConfig))
            val medRaster = GDALWarp.executeWarp(
                resultFileName,
                Seq(raster),
                command = s"gdalwarp -r med -tr $width $height -of $driverShortName",
                Option(exprConfig)
            )

            // Max pixel is a hack since we get a 1x1 tile back
            val nBands = raster.getDatasetOrNull().GetRasterCount()
            val values = (1 to nBands).map(medRaster.getBand(_).maxPixelValue) // <- max from median 1x1 result
            ArrayData.toArrayData(values.toArray)
        }.getOrElse(ArrayData.toArrayData(Array.empty[Double]))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Median extends WithExpressionInfo {

    override def name: String = "rst_median"

    override def usage: String = "_FUNC_(expr1) - Returns an array containing mean values for each band."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |       [1.123, 2.123, 3.123]
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Median](1, exprConfig)
    }

}
