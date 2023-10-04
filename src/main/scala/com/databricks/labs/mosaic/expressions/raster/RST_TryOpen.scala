package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types._

/** Returns true if the raster is empty. */
case class RST_TryOpen(raster: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_TryOpen](raster, BooleanType, returnsRaster = false, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    /** Returns true if the raster can be opened. */
    override def rasterTransform(raster: MosaicRaster): Any = {
        Option(raster.getRaster).isDefined
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_TryOpen extends WithExpressionInfo {

    override def name: String = "rst_tryopen"

    override def usage: String = "_FUNC_(expr1) - Returns true if the raster can be opened."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |        false
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_TryOpen](1, expressionConfig)
    }

}
