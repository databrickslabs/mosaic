package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types._

/** Returns true if the tile is empty. */
case class RST_IsEmpty(raster: Expression, exprConfig: ExprConfig)
    extends RasterExpression[RST_IsEmpty](raster, returnsRaster = false, exprConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = BooleanType

    /** Returns true if the tile is empty. */
    override def rasterTransform(tile: RasterTile): Any = {
        val raster = tile.raster
        (raster.ySize == 0 && raster.xSize == 0) || raster.isEmpty
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_IsEmpty extends WithExpressionInfo {

    override def name: String = "rst_isempty"

    override def usage: String = "_FUNC_(expr1) - Returns true if the tile tile is empty."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |        false
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_IsEmpty](1, exprConfig)
    }

}
