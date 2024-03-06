package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.Raster1ArgExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

/** The expression for applying kernel filter on a raster. */
case class RST_Convolve(
    rastersExpr: Expression,
    kernelExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends Raster1ArgExpression[RST_Convolve](
      rastersExpr,
      kernelExpr,
      returnsRaster = true,
      expressionConfig = expressionConfig
    )
      with NullIntolerant
      with CodegenFallback {

    override def dataType: org.apache.spark.sql.types.DataType = RasterTileType(expressionConfig.getCellIdType, rastersExpr)

    val geometryAPI: GeometryAPI = GeometryAPI(expressionConfig.getGeometryAPI)

    /**
      * Clips a raster by a vector.
      *
      * @param tile
      *   The raster to be used.
      * @param arg1
      *   The vector to be used.
      * @return
      *   The clipped raster.
      */
    override def rasterTransform(tile: MosaicRasterTile, arg1: Any): Any = {
        val kernel = arg1.asInstanceOf[ArrayData].array.map(_.asInstanceOf[ArrayData].array.map(
          el => kernelExpr.dataType match {
              case ArrayType(ArrayType(DoubleType, false), false) => el.asInstanceOf[Double]
              case ArrayType(ArrayType(DecimalType(), false), false) => el.asInstanceOf[java.math.BigDecimal].doubleValue()
              case ArrayType(ArrayType(IntegerType, false), false) => el.asInstanceOf[Int].toDouble
              case _ => throw new IllegalArgumentException(s"Unsupported kernel type: ${kernelExpr.dataType}")
          }
        ))
        tile.copy(
          raster = tile.getRaster.convolve(kernel)
        )
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Convolve extends WithExpressionInfo {

    override def name: String = "rst_convolve"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns a raster with the kernel filter applied.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster, kernel);
          |        {index_id, clipped_raster, parentPath, driver}
          |        {index_id, clipped_raster, parentPath, driver}
          |        ...
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Convolve](2, expressionConfig)
    }

}
