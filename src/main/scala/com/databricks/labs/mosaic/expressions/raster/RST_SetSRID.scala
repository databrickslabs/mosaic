package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.Raster1ArgExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types.DataType

/** The expression for clipping a raster by a vector. */
case class RST_SetSRID(
    rastersExpr: Expression,
    sridExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends Raster1ArgExpression[RST_SetSRID](
      rastersExpr,
      sridExpr,
      returnsRaster = true,
      expressionConfig = expressionConfig
    )
      with NullIntolerant
      with CodegenFallback {

    // serialize data type
    override def dataType: DataType = {
        RasterTileType(expressionConfig.getCellIdType, rastersExpr, expressionConfig.isRasterUseCheckpoint)
    }

    val geometryAPI: GeometryAPI = GeometryAPI(expressionConfig.getGeometryAPI)

    /**
      * Sets the SRID of raster tiles.
      *
      * @param tile
      *   The raster to be used.
      * @param arg1
      *   The SRID to be used.
      * @return
      *   The updated raster tile.
      */
    override def rasterTransform(tile: MosaicRasterTile, arg1: Any): Any = {

        // set srid on the raster
        // - this is an in-place operation as of 0.4.3+
        val raster = tile.getRaster
        raster.setSRID(arg1.asInstanceOf[Int])
        // create a new object for the return
        tile.copy(raster = MosaicRasterGDAL(null, raster.getCreateInfo, raster.getMemSize))
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_SetSRID extends WithExpressionInfo {

    override def name: String = "rst_setsrid"

    override def usage: String =
        """
          |_FUNC_(expr1) - Force set the SRID of a raster.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster, srid);
          |        {index_id, raster, parentPath, driver}
          |        {index_id, raster, parentPath, driver}
          |        ...
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_SetSRID](2, expressionConfig)
    }

}
