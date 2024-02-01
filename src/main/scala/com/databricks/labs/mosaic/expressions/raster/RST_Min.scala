package com.databricks.labs.mosaic.expressions.raster

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
case class RST_Min(raster: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_Min](raster, returnsRaster = false, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = ArrayType(DoubleType)

    /** Returns the upper left x of the raster. */
    override def rasterTransform(tile: MosaicRasterTile): Any = {
        val nBands = tile.raster.raster.GetRasterCount()
        val minValues = (1 to nBands).map(tile.raster.getBand(_).minPixelValue)
        ArrayData.toArrayData(minValues.toArray)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Min extends WithExpressionInfo {

    override def name: String = "rst_min"

    override def usage: String = "_FUNC_(expr1) - Returns an array containing min values for each band."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |       [1.123, 2.123, 3.123]
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Min](1, expressionConfig)
    }

}
