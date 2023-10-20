package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.NDVI
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.Raster2ArgExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types.BinaryType

/**
  * Returns a set of new rasters with the specified tile size (tileWidth x
  * tileHeight).
  */
case class RST_NDVI(
    rastersExpr: Expression,
    redIndex: Expression,
    nirIndex: Expression,
    expressionConfig: MosaicExpressionConfig
) extends Raster2ArgExpression[RST_NDVI](
      rastersExpr,
      redIndex,
      nirIndex,
      BinaryType,
      returnsRaster = true,
      expressionConfig = expressionConfig
    )
      with NullIntolerant
      with CodegenFallback {

    /**
      * The function to be overridden by the extending class. It is called when
      * the expression is evaluated. It provides the raster and the arguments to
      * the expression. It abstracts spark serialization from the caller.
      *
      * @param raster
      *   The raster to be used.
      * @param arg1
      *   The first argument.
      * @param arg2
      *   The second argument.
      * @return
      *   A result of the expression.
      */
    override def rasterTransform(raster: MosaicRasterGDAL, arg1: Any, arg2: Any): Any = {
        val redInd = arg1.asInstanceOf[Int]
        val nirInd = arg2.asInstanceOf[Int]
        NDVI.compute(raster, redInd, nirInd)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_NDVI extends WithExpressionInfo {

    override def name: String = "rst_ndvi"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns a raster contains NDVI index computed by bands provided by red_index and nir_index.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, b);
          |        /path/to/raster_tile_1.tif
          |        /path/to/raster_tile_2.tif
          |        /path/to/raster_tile_3.tif
          |        ...
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_NDVI](3, expressionConfig)
    }

}
