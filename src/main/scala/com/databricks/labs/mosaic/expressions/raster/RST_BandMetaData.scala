package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.gdal.RasterBandGDAL
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterBandExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types._

/**
  * The expression for extracting metadata from a tile band.
  * @param raster
  *   The expression for the tile. If the tile is stored on disk, the path
  *   to the tile is provided. If the tile is stored in memory, the bytes of
  *   the tile are provided.
  * @param band
  *   The band index.
  * @param exprConfig
  *   Additional arguments for the expression (expressionConfigs).
  */
case class RST_BandMetaData(raster: Expression, band: Expression, exprConfig: ExprConfig)
    extends RasterBandExpression[RST_BandMetaData](
      raster,
      band,
      returnsRaster = false,
      exprConfig = exprConfig
    )
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = MapType(StringType, StringType)

    /**
      * @param tile
      *   The tile to be used.
      * @param band
      *   The band to be used.
      * @return
      *   The band metadata of the band as a map type result.
      */
    override def bandTransform(tile: RasterTile, band: RasterBandGDAL): Any = {
        buildMapString(band.metadata)
    }
}

/** Expression info required for the expression registration for spark SQL. */
object RST_BandMetaData extends WithExpressionInfo {

    override def name: String = "rst_bandmetadata"

    override def usage: String = "_FUNC_(expr1, expr2) - Extracts metadata from a tile tile band."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile, 1);
          |        {"NC_GLOBAL#acknowledgement":"NOAA Coral Reef Watch Program","NC_GLOBAL#cdm_data_type":"Grid"}
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_BandMetaData](2, exprConfig)
    }

}
