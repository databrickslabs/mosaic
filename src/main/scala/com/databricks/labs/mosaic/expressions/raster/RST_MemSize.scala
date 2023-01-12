package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

/** Returns the memory size of the raster in bytes. */
case class RST_MemSize(path: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_MemSize](path, LongType, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    /** Returns the memory size of the raster in bytes. */
    override def rasterTransform(raster: MosaicRaster): Any = raster.getMemSize

}

/** Expression info required for the expression registration for spark SQL. */
object RST_MemSize extends WithExpressionInfo {

    override def name: String = "rst_memsize"

    override def usage: String = "_FUNC_(expr1) - Returns number of bytes for in memory representation of the raster."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |        228743
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_MemSize](1, expressionConfig)
    }

}
