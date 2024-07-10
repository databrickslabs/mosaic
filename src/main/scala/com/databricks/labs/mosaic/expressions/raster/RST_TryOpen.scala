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
case class RST_TryOpen(raster: Expression, exprConfig: ExprConfig)
    extends RasterExpression[RST_TryOpen](raster, returnsRaster = false, exprConfig)
        with NullIntolerant
        with CodegenFallback {

    override def dataType: DataType = BooleanType

    /** Returns true if the tile can be opened. */
    override def rasterTransform(tile: RasterTile): Any = {
        tile.raster.withDatasetHydratedOpt().isDefined
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_TryOpen extends WithExpressionInfo {

    override def name: String = "rst_tryopen"

    override def usage: String = "_FUNC_(expr1) - Returns true if the tile tile can be opened."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |        false
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_TryOpen](1, exprConfig)
    }

}