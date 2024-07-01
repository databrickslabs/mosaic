package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types._

/** Returns the skew x of the raster. */
case class RST_SkewX(raster: Expression, exprConfig: ExprConfig)
    extends RasterExpression[RST_SkewX](raster, returnsRaster = false, exprConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = DoubleType

    /** Returns the skew x of the raster, default 0. */
    override def rasterTransform(tile: RasterTile): Any = {
        tile.raster.withDatasetHydratedOpt() match {
            case Some(dataset) => dataset.GetGeoTransform()(2)
            case _ => 0d // double
        }
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_SkewX extends WithExpressionInfo {

    override def name: String = "rst_skewx"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns skew X in the raster tile.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |        1.123
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_SkewX](1, exprConfig)
    }

}
