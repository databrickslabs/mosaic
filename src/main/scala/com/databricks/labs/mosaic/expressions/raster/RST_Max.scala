package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

import scala.util.Try


/** Returns the max value per band of the tile. */
case class RST_Max(raster: Expression, exprConfig: ExprConfig)
    extends RasterExpression[RST_Max](raster, returnsRaster = false, exprConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = ArrayType(DoubleType)

    /** Returns the max value per band of the tile. */
    override def rasterTransform(tile: RasterTile): Any = Try {
        val raster = tile.raster
        val nBands = raster.getDatasetOrNull().GetRasterCount()
        val maxValues = (1 to nBands).map(raster.getBand(_).maxPixelValue)
        ArrayData.toArrayData(maxValues.toArray)
    }.getOrElse(ArrayData.toArrayData(Array.empty[Double]))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Max extends WithExpressionInfo {

    override def name: String = "rst_max"

    override def usage: String = "_FUNC_(expr1) - Returns an array containing max values for each band."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |       [1.123, 2.123, 3.123]
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Max](1, exprConfig)
    }

}
