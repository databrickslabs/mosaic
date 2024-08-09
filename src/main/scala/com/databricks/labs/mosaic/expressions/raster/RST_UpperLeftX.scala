package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types._

/** Returns the upper left x of the tile. */
case class RST_UpperLeftX(raster: Expression, exprConfig: ExprConfig)
    extends RasterExpression[RST_UpperLeftX](raster, returnsRaster = false, exprConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = DoubleType

    /** Returns the upper left x of the tile. */
    override def rasterTransform(tile: RasterTile): Any = tile.raster.originX

}

/** Expression info required for the expression registration for spark SQL. */
object RST_UpperLeftX extends WithExpressionInfo {

    override def name: String = "rst_upperleftx"

    override def usage: String = "_FUNC_(expr1) - Returns upper left x coordinate of the tile tile."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |       1.123
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_UpperLeftX](1, exprConfig)
    }

}
