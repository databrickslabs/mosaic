package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types._

/** Returns the memory size of the raster in bytes. */
case class RST_MemSize(raster: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_MemSize](raster, returnsRaster = false, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = LongType

    /** Returns the memory size of the raster in bytes. */
    override def rasterTransform(tile: MosaicRasterTile): Any = tile.getRaster.getMemSize

}

/** Expression info required for the expression registration for spark SQL. */
object RST_MemSize extends WithExpressionInfo {

    override def name: String = "rst_memsize"

    override def usage: String = "_FUNC_(expr1) - Returns number of bytes for in memory representation of the raster tile."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |        228743
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_MemSize](1, expressionConfig)
    }

}
