package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.Raster1ArgExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types.DataType

/** The expression for clipping a tile by a vector. */
case class RST_SetSRID(
                          rastersExpr: Expression,
                          sridExpr: Expression,
                          exprConfig: ExprConfig
) extends Raster1ArgExpression[RST_SetSRID](
      rastersExpr,
      sridExpr,
      returnsRaster = true,
      exprConfig = exprConfig
    )
      with NullIntolerant
      with CodegenFallback {

    // serialize data type
    override def dataType: DataType = {
        RasterTileType(exprConfig.getCellIdType, rastersExpr, exprConfig.isRasterUseCheckpoint)
    }

    val geometryAPI: GeometryAPI = GeometryAPI(exprConfig.getGeometryAPI)

    /**
      * Sets the SRID of tile tiles.
      *
      * @param tile
      *   The tile to be used.
      * @param arg1
      *   The SRID to be used.
      * @return
      *   The updated tile tile.
      */
    override def rasterTransform(tile: RasterTile, arg1: Any): Any = {

        // set srid on the tile
        // - this is an in-place operation as of 0.4.3+
        // create a new object for the return
        tile.copy(raster = tile.raster.setSRID(arg1.asInstanceOf[Int]))
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_SetSRID extends WithExpressionInfo {

    override def name: String = "rst_setsrid"

    override def usage: String =
        """
          |_FUNC_(expr1) - Force set the SRID of a tile.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(tile, srid);
          |        {index_id, tile, parentPath, driver}
          |        {index_id, tile, parentPath, driver}
          |        ...
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_SetSRID](2, exprConfig)
    }

}
