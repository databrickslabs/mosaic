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
import org.apache.spark.unsafe.types.UTF8String

/** Returns the data type of the raster. */
case class RST_Type(raster: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_Type](raster, returnsRaster = false, expressionConfig)
        with NullIntolerant
        with CodegenFallback {

    override def dataType: DataType = ArrayType(StringType)

    /** Returns the data type of the raster. */
    override def rasterTransform(tile: MosaicRasterTile): Any = {
        //loop over each band in the raster and get the data type
        val dataTypeStrings = tile.getRaster.getBands.map(_.dataTypeHuman).map(UTF8String.fromString)
        ArrayData.toArrayData(dataTypeStrings.toArray)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Type extends WithExpressionInfo {

    override def name: String = "rst_type"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns an array of data types for each band in the raster tile.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |        [UInt16]
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Type](1, expressionConfig)
    }

}
