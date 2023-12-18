package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

/** Returns true if the raster is empty. */
case class RST_IsEmpty(path: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_IsEmpty](path, BooleanType, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    /** Returns true if the raster is empty. */
    override def rasterTransform(raster: MosaicRaster): Any = raster.ySize == 0 && raster.xSize == 0

}

/** Expression info required for the expression registration for spark SQL. */
object RST_IsEmpty extends WithExpressionInfo {

    override def name: String = "rst_isempty"

    override def usage: String = "_FUNC_(expr1) - Returns true if the raster is empty."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |        false
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_IsEmpty](2, expressionConfig)
    }

}
