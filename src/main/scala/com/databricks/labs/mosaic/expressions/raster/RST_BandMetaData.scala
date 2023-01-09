package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.{MosaicRaster, MosaicRasterBand}
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterBandExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

/**
  * The expression for extracting metadata from a raster band.
  * @param path
  *   The path to the raster.
  * @param band
  *   The band index.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  */
case class RST_BandMetaData(path: Expression, band: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterBandExpression[RST_BandMetaData](path, band, MapType(StringType, StringType), expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    /**
      * @param raster
      *   The raster to be used.
      * @param band
      *   The band to be used.
      * @return
      *   The band metadata of the band as a map type result.
      */
    override def bandTransform(raster: MosaicRaster, band: MosaicRasterBand): Any = {
        val metaData = band.metadata
        buildMapString(metaData)
    }
}

/** Expression info required for the expression registration for spark SQL. */
object RST_BandMetaData extends WithExpressionInfo {

    override def name: String = "rst_bandmetadata"

    override def usage: String = "_FUNC_(expr1, expr2) - Extracts metadata from a raster band."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, 1);
          |        {"NC_GLOBAL#acknowledgement":"NOAA Coral Reef Watch Program","NC_GLOBAL#cdm_data_type":"Grid"}
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_BandMetaData](2, expressionConfig)
    }

}
