package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types._

/** Returns the metadata of the tile. */
case class RST_MetaData(raster: Expression, exprConfig: ExprConfig)
    extends RasterExpression[RST_MetaData](raster, returnsRaster = false, exprConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = MapType(StringType, StringType)

    /** Returns the metadata of the tile. */
    override def rasterTransform(tile: RasterTile): Any = buildMapString(tile.raster.metadata)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_MetaData extends WithExpressionInfo {

    override def name: String = "rst_metadata"

    override def usage: String = "_FUNC_(expr1) - Extracts metadata from a tile tile dataset."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |        {"NC_GLOBAL#acknowledgement":"NOAA Coral Reef Watch Program","NC_GLOBAL#cdm_data_type":"Grid"}
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_MetaData](1, exprConfig)
    }

}
