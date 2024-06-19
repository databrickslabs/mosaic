package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types._

/** Returns the metadata of the raster. */
case class RST_MetaData(raster: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_MetaData](raster, returnsRaster = false, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = MapType(StringType, StringType)

    /** Returns the metadata of the raster. */
    override def rasterTransform(tile: MosaicRasterTile): Any = buildMapString(tile.getRaster.metadata)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_MetaData extends WithExpressionInfo {

    override def name: String = "rst_metadata"

    override def usage: String = "_FUNC_(expr1) - Extracts metadata from a raster tile dataset."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |        {"NC_GLOBAL#acknowledgement":"NOAA Coral Reef Watch Program","NC_GLOBAL#cdm_data_type":"Grid"}
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_MetaData](1, expressionConfig)
    }

}
