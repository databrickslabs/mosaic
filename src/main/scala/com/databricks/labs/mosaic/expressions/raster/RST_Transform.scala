package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.operator.proj.RasterProject
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.Raster1ArgExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types._
import org.gdal.osr.SpatialReference

/** Returns the upper left x of the raster. */
case class RST_Transform(
    tileExpr: Expression,
    srid: Expression,
    expressionConfig: MosaicExpressionConfig
) extends Raster1ArgExpression[RST_Transform](
      tileExpr,
      srid,
      returnsRaster = true,
      expressionConfig
    )
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = {
        GDAL.enable(expressionConfig)
        RasterTileType(expressionConfig.getCellIdType, tileExpr, expressionConfig.isRasterUseCheckpoint)
    }

    /** Returns the upper left x of the raster. */
    override def rasterTransform(tile: MosaicRasterTile, arg1: Any): Any = {
        val srid = arg1.asInstanceOf[Int]
        val sReff = new SpatialReference()
        sReff.ImportFromEPSG(srid)
        sReff.SetAxisMappingStrategy(org.gdal.osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        val result = RasterProject.project(tile.raster, sReff)
        tile.copy(raster = result)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Transform extends WithExpressionInfo {

    override def name: String = "rst_transform"

    override def usage: String = "_FUNC_(expr1) - Returns an array containing mean values for each band."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |       [1.123, 2.123, 3.123]
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Transform](2, expressionConfig)
    }

}
