package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterBandGDAL
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterBandExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types._

/**
  * The expression for extracting metadata from a raster band.
  * @param raster
  *   The expression for the raster. If the raster is stored on disk, the path
  *   to the raster is provided. If the raster is stored in memory, the bytes of
  *   the raster are provided.
  * @param band
  *   The band index.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  */
case class RST_BandMetaData(raster: Expression, band: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterBandExpression[RST_BandMetaData](
      raster,
      band,
      returnsRaster = false,
      expressionConfig = expressionConfig
    )
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = MapType(StringType, StringType)

    /**
      * @param raster
      *   The raster to be used.
      * @param band
      *   The band to be used.
      * @return
      *   The band metadata of the band as a map type result.
      */
    override def bandTransform(raster: MosaicRasterTile, band: MosaicRasterBandGDAL): Any = {
        buildMapString(band.metadata)
    }
}

/** Expression info required for the expression registration for spark SQL. */
object RST_BandMetaData extends WithExpressionInfo {

    override def name: String = "rst_bandmetadata"

    override def usage: String = "_FUNC_(expr1, expr2) - Extracts metadata from a raster tile band."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile, 1);
          |        {"NC_GLOBAL#acknowledgement":"NOAA Coral Reef Watch Program","NC_GLOBAL#cdm_data_type":"Grid"}
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_BandMetaData](2, expressionConfig)
    }

}
