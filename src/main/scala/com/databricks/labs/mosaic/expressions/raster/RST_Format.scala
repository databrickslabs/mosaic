package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

case class RST_Format (
                              tileExpr: Expression,
                              expressionConfig: MosaicExpressionConfig
                          ) extends RasterExpression[RST_Format](
    tileExpr,
    returnsRaster = false,
    expressionConfig
)
    with NullIntolerant
    with CodegenFallback {

    override def dataType: DataType = StringType

    /** Returns the format of the raster. */
    override def rasterTransform(tile: MosaicRasterTile): Any = {
        UTF8String.fromString(tile.getDriver)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Format extends WithExpressionInfo {

    override def name: String = "rst_format"

    override def usage: String = "_FUNC_(expr1) - Returns the driver used to read the raster"

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(tile)
          |       'GTiff'
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Format](1, expressionConfig)
    }

}
