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


/** Returns the min value per band of the tile. */
case class RST_Min(raster: Expression, exprConfig: ExprConfig)
    extends RasterExpression[RST_Min](raster, returnsRaster = false, exprConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = ArrayType(DoubleType)

    /** Returns the min value per band of the tile. */
    override def rasterTransform(tile: RasterTile): Any = {
        val raster = tile.raster
        raster.getDatasetOpt() match {
            case Some(dataset) =>
                val nBands = dataset.GetRasterCount()
                val minValues = (1 to nBands).map(raster.getBand (_).minPixelValue)
                ArrayData.toArrayData(minValues.toArray)
            case _ => ArrayData.toArrayData(Array.empty[Double])
        }
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Min extends WithExpressionInfo {

    override def name: String = "rst_min"

    override def usage: String = "_FUNC_(expr1) - Returns an array containing min values for each band."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |       [1.123, 2.123, 3.123]
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Min](1, exprConfig)
    }

}
