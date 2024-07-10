package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types._

/** Returns the scale x of the tile. */
case class RST_ScaleX(raster: Expression, exprConfig: ExprConfig)
    extends RasterExpression[RST_ScaleX](raster, returnsRaster = false, exprConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = DoubleType

    /** Returns the scale x of the tile. */
    override def rasterTransform(tile: RasterTile): Any = tile.raster.pixelXSize

}

/** Expression info required for the expression registration for spark SQL. */
object RST_ScaleX extends WithExpressionInfo {

    override def name: String = "rst_scalex"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns scale X in the tile tile.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |        1.123
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_ScaleX](1, exprConfig)
    }

}
