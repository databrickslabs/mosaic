package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALWarp
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import com.databricks.labs.mosaic.utils.PathUtils
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

/** Returns the upper left x of the raster. */
case class RST_Median(rasterExpr: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_Median](rasterExpr, returnsRaster = false, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = ArrayType(DoubleType)

    /** Returns the upper left x of the raster. */
    override def rasterTransform(tile: MosaicRasterTile): Any = {
        val raster = tile.raster
        val width = raster.xSize * raster.pixelXSize
        val height = raster.ySize * raster.pixelYSize
        val outShortName = raster.getDriversShortName
        val resultFileName = PathUtils.createTmpFilePath(GDAL.getExtension(outShortName))
        val medRaster = GDALWarp.executeWarp(
          resultFileName,
          Seq(raster),
          command = s"gdalwarp -r med -tr $width $height -of $outShortName"
        )
        // Max pixel is a hack since we get a 1x1 raster back
        val maxValues = (1 to medRaster.raster.GetRasterCount()).map(medRaster.getBand(_).maxPixelValue)
        ArrayData.toArrayData(maxValues.toArray)
    }

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

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Median](1, expressionConfig)
    }

}
