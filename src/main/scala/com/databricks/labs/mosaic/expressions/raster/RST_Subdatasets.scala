package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

/** Returns the subdatasets of the raster. */
case class RST_Subdatasets(path: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_Subdatasets](path, MapType(StringType, StringType), expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    /** Returns the subdatasets of the raster. */
    override def rasterTransform(raster: MosaicRaster): Any = {
        val subdatasets = raster.subdatasets
        buildMapString(subdatasets)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Subdatasets extends WithExpressionInfo {

    override def name: String = "rst_subdatasets"

    override def usage: String = "_FUNC_(expr1) - Extracts subdataset paths and descriptions from a raster dataset."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |        {"NETCDF:"ct5km_baa-max-7d_v3.1_20220101.nc":bleaching_alert_area":"[1x3600x7200] N/A (8-bit unsigned integer)",
          |        "NETCDF:"ct5km_baa-max-7d_v3.1_20220101.nc":mask":"[1x3600x7200] mask (8-bit unsigned integer)"}
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Subdatasets](1, expressionConfig)
    }

}
