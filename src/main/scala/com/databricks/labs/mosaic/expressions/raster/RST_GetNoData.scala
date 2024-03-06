package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType}

/** The expression for extracting the no data value of a raster. */
case class RST_GetNoData(
    rastersExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends RasterExpression[RST_GetNoData](
      rastersExpr,
      returnsRaster = false,
      expressionConfig = expressionConfig
    )
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = ArrayType(DoubleType)

    /**
      * Extracts the no data value of a raster.
      *
      * @param tile
      *   The raster to be used.
      * @return
      *   The no data value of the raster.
      */
    override def rasterTransform(tile: MosaicRasterTile): Any = {
        ArrayData.toArrayData(tile.getRaster.getBands.map(_.noDataValue))
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_GetNoData extends WithExpressionInfo {

    override def name: String = "rst_getnodata"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns no data values for raster tile bands.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |        [0.0, -9999.0, ...]
          |        ...
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_GetNoData](1, expressionConfig)
    }

}
