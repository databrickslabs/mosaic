package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types._

/** Returns the number of bands in the raster. */
case class RST_NumBands(raster: Expression, exprConfig: ExprConfig)
    extends RasterExpression[RST_NumBands](raster, returnsRaster = false, exprConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = IntegerType

    /** Returns the number of bands in the raster. */
    override def rasterTransform(tile: RasterTile): Any = tile.raster.numBands

}

/** Expression info required for the expression registration for spark SQL. */
object RST_NumBands extends WithExpressionInfo {

    override def name: String = "rst_numbands"

    override def usage: String = "_FUNC_(expr1) - Returns number of bands in the raster tile."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |        4
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_NumBands](1, exprConfig)
    }

}
