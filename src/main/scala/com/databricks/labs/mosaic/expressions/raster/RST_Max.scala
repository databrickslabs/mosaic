package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALInfo
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._


/** Returns the upper left x of the raster. */
case class RST_Max(raster: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_Max](raster, returnsRaster = false, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = ArrayType(DoubleType)

    /** Returns the upper left x of the raster. */
    override def rasterTransform(tile: MosaicRasterTile): Any = {
        val nBands = tile.raster.raster.GetRasterCount()
        val maxValues = (1 to nBands).map(tile.raster.getBand(_).maxPixelValue)
        ArrayData.toArrayData(maxValues.toArray)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Max extends WithExpressionInfo {

    override def name: String = "rst_max"

    override def usage: String = "_FUNC_(expr1) - Returns an array containing max values for each band."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |       [1.123, 2.123, 3.123]
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Max](1, expressionConfig)
    }

}
