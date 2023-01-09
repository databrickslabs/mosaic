package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

/** Returns the metadata of the raster. */
case class RST_MetaData(path: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_MetaData](path, MapType(StringType, StringType), expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    /** Returns the metadata of the raster. */
    override def rasterTransform(raster: MosaicRaster): Any = {
        val metaData = raster.metadata
        buildMapString(metaData)
    }
}

/** Expression info required for the expression registration for spark SQL. */
object RST_MetaData extends WithExpressionInfo {

    override def name: String = "rst_metadata"

    override def usage: String = "_FUNC_(expr1) - Extracts metadata from a raster dataset."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |        {"NC_GLOBAL#acknowledgement":"NOAA Coral Reef Watch Program","NC_GLOBAL#cdm_data_type":"Grid"}
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_MetaData](1, expressionConfig)
    }

}
