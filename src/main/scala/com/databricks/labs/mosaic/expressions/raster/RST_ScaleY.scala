package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

/** Returns the scale y of the raster. */
case class RST_ScaleY(path: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_ScaleY](path, DoubleType, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    /** Returns the scale y of the raster. */
    override def rasterTransform(raster: MosaicRaster): Any = {
        val scaleY = raster.getRaster.GetGeoTransform()(5)
        scaleY
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_ScaleY extends WithExpressionInfo {

    override def name: String = "rst_scaley"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns scale Y in the raster.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |        1.123
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_ScaleY](1, expressionConfig)
    }

}
