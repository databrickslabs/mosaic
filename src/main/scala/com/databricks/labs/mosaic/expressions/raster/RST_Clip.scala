package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.operator.clip.RasterClipByVector
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.WithExpressionInfo
import com.databricks.labs.mosaic.expressions.raster.base.Raster2ArgExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, NullIntolerant}
import org.apache.spark.sql.types.BooleanType

import scala.util.Try

/** The expression for clipping a raster by a vector. */
case class RST_Clip(
    rastersExpr: Expression,
    geometryExpr: Expression,
    cutlineAllTouchedExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends Raster2ArgExpression[RST_Clip](
      rastersExpr,
      geometryExpr,
      cutlineAllTouchedExpr,
      returnsRaster = true,
      expressionConfig = expressionConfig
    )
      with NullIntolerant
      with CodegenFallback {

    val geometryAPI: GeometryAPI = GeometryAPI(expressionConfig.getGeometryAPI)

    override def dataType: org.apache.spark.sql.types.DataType = {
        GDAL.enable(expressionConfig)
        RasterTileType(expressionConfig.getCellIdType, rastersExpr, expressionConfig.isRasterUseCheckpoint)
    }

    /**
      * Clips a raster by a vector.
      *
      * @param tile
      *   The raster to be used.
      * @param arg1
      *   The vector to be used.
      * @param arg2
      *   cutline handling (boolean).
      * @return
      *   The clipped raster.
      */
    override def rasterTransform(tile: MosaicRasterTile, arg1: Any, arg2: Any): Any = {
        val geometry = geometryAPI.geometry(arg1, geometryExpr.dataType)
        val geomCRS = geometry.getSpatialReferenceOSR
        val cutlineAllTouched = arg2.asInstanceOf[Boolean]
        tile.copy(
          raster = RasterClipByVector.clip(tile.getRaster, geometry, geomCRS, geometryAPI, cutlineAllTouched)
        )
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Clip extends WithExpressionInfo {

    override def name: String = "rst_clip"

    override def usage: String =
        """
          |_FUNC_(expr1, expr2) - Returns a raster tile clipped by provided vector.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile, vector);
          |        {index_id, clipped_raster, parentPath, driver}
          |        {index_id, clipped_raster, parentPath, driver}
          |        ...
          |  """.stripMargin

    override def builder(exprConfig: MosaicExpressionConfig): FunctionBuilder = { (children: Seq[Expression]) =>
    {
        def checkCutline(cutline: Expression): Boolean = Try(cutline.eval().asInstanceOf[Boolean]).isSuccess
        val noCutlineArg = new Literal(true, BooleanType) // default is true for tessellation needs

        children match {
            // Note type checking only works for literals
            case Seq(input, vector)                                   =>
                RST_Clip(input, vector, noCutlineArg, exprConfig)
            case Seq(input, vector, cutline) if checkCutline(cutline) =>
                RST_Clip(input, vector, cutline, exprConfig)
            case _ => RST_Clip(children.head, children(1), children(2), exprConfig)
        }
    }
    }

}
