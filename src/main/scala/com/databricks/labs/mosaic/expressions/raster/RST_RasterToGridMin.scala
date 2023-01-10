package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterToGridExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DoubleType

/** Returns the minimum value of the raster in the grid cell. */
case class RST_RasterToGridMin(
    path: Expression,
    resolution: Expression,
    expressionConfig: MosaicExpressionConfig
) extends RasterToGridExpression[RST_RasterToGridMin, Double](
      path,
      resolution,
      DoubleType,
      expressionConfig
    )
      with NullIntolerant
      with CodegenFallback {

    /** Returns the minimum value of the raster in the grid cell. */
    override def valuesCombiner(values: Seq[Double]): Double = values.min

}

/** Expression info required for the expression registration for spark SQL. */
object RST_RasterToGridMin extends WithExpressionInfo {

    override def name: String = "rst_rastertogridmin"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns a collection of grid index cells with the min pixel value per cell for each band of the raster.
          |                The output type is array<array<struct<index: long, measure: double>>>.
          |                Raster mask is taken into account and only valid pixels are used for the calculation.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |        [[(11223344, 123.4), (11223345, 125.4), ...], [(11223344, 123.1), (11223344, 123.6) ...], ...]
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_RasterToGridMin](2, expressionConfig)
    }

}
