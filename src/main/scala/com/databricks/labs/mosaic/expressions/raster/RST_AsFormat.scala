package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.operator.RasterTranslate.TranslateFormat
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.Raster1ArgExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

case class RST_AsFormat (
                              tileExpr: Expression,
                              newFormat: Expression,
                              expressionConfig: MosaicExpressionConfig
                          ) extends Raster1ArgExpression[RST_AsFormat](
    tileExpr,
    newFormat,
    returnsRaster = true,
    expressionConfig
)
    with NullIntolerant
    with CodegenFallback {

    override def dataType: DataType = {
        GDAL.enable(expressionConfig)
        RasterTileType(expressionConfig.getCellIdType, tileExpr, expressionConfig.isRasterUseCheckpoint)
    }

    /** Changes the data type of a band of the raster. */
    override def rasterTransform(tile: MosaicRasterTile, arg1: Any): Any = {

        val newFormat = arg1.asInstanceOf[UTF8String].toString
        if (tile.getRaster.driverShortName.getOrElse("") == newFormat) {
            return tile
        }
        val result = TranslateFormat.update(tile.getRaster, newFormat)
        tile.copy(raster = result).setDriver(newFormat)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_AsFormat extends WithExpressionInfo {

    override def name: String = "rst_asformat"

    override def usage: String = "_FUNC_(expr1) - Returns a raster tile in a different underlying format"

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(tile, 'GTiff')
          |       {index_id, updated_raster, parentPath, driver}
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_AsFormat](2, expressionConfig)
    }

}
