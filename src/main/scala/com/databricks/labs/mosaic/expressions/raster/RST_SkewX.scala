package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

/** Returns the skew x of the raster. */
case class RST_SkewX(path: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_SkewX](path, DoubleType, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    /** Returns the skew x of the raster. */
    override def rasterTransform(raster: MosaicRaster): Any = {
        val skewX = raster.getRaster.GetGeoTransform()(2)
        skewX
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_SkewX extends WithExpressionInfo {

    override def name: String = "rst_skewx"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns skew X in the raster.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |        1.123
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_SkewX](1, expressionConfig)
    }

}
