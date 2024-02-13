package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.operator.merge.MergeBands
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterArrayExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types.ArrayType

/** The expression for stacking and resampling input bands. */
case class RST_FromBands(
    bandsExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends RasterArrayExpression[RST_FromBands](
      bandsExpr,
      returnsRaster = true,
      expressionConfig = expressionConfig
    )
      with NullIntolerant
      with CodegenFallback {

    override def dataType: org.apache.spark.sql.types.DataType =
        RasterTileType(
          expressionConfig.getCellIdType,
          RasterTileType(bandsExpr).rasterType
        )

    /**
      * Stacks and resamples input bands.
      * @param rasters
      *   The rasters to be used.
      * @return
      *   The stacked and resampled raster.
      */
    override def rasterTransform(rasters: Seq[MosaicRasterTile]): Any = {
        rasters.head.copy(raster = MergeBands.merge(rasters.map(_.getRaster), "bilinear"))
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_FromBands extends WithExpressionInfo {

    override def name: String = "rst_frombands"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns raster tiles that are a result of stacking and resampling input bands.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(array(band1, band2, band3));
          |        {index_id, raster, parent_path, driver}
          |        {index_id, raster, parent_path, driver}
          |        ...
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_FromBands](1, expressionConfig)
    }

}
