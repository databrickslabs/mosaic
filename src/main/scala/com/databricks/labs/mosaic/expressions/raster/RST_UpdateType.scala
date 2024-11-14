package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.operator.RasterTranslate.TranslateType
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.Raster1ArgExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

case class RST_UpdateType (
                              tileExpr: Expression,
                              newType: Expression,
                              expressionConfig: MosaicExpressionConfig
                          ) extends Raster1ArgExpression[RST_UpdateType](
    tileExpr,
    newType,
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

        val newType = arg1.asInstanceOf[UTF8String].toString
        val result = TranslateType.update(tile.getRaster, newType)
        tile.copy(raster = result)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_UpdateType extends WithExpressionInfo {

    override def name: String = "rst_updatetype"

    override def usage: String = "_FUNC_(expr1) - Returns a raster tile with an updated data type"

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(tile, 'Float32')
          |       {index_id, updated_raster, parentPath, driver}
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_UpdateType](2, expressionConfig)
    }

}
