package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types._

/** Returns the subdatasets of the raster. */
case class RST_Subdatasets(raster: Expression, exprConfig: ExprConfig)
    extends RasterExpression[RST_Subdatasets](
      raster,
      returnsRaster = false,
      exprConfig
    )
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = MapType(StringType, StringType)

    /** Returns the subdatasets of the raster. */
    override def rasterTransform(tile: RasterTile): Any = buildMapString(tile.raster.subdatasets)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Subdatasets extends WithExpressionInfo {

    override def name: String = "rst_subdatasets"

    override def usage: String = "_FUNC_(expr1) - Extracts subdataset paths and descriptions from a raster tile dataset."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |        {"NETCDF:"ct5km_baa-max-7d_v3.1_20220101.nc":bleaching_alert_area":"[1x3600x7200] N/A (8-bit unsigned integer)",
          |        "NETCDF:"ct5km_baa-max-7d_v3.1_20220101.nc":mask":"[1x3600x7200] mask (8-bit unsigned integer)"}
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Subdatasets](1, exprConfig)
    }

}
