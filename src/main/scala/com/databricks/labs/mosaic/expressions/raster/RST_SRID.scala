package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types._
import org.gdal.osr.SpatialReference

import scala.util.Try

/** Returns the SRID of the raster. */
case class RST_SRID(raster: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_SRID](raster, returnsRaster = false, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = IntegerType

    /** Returns the SRID of the raster. */
    override def rasterTransform(tile: MosaicRasterTile): Any = {
        // Reference: https://gis.stackexchange.com/questions/267321/extracting-epsg-from-a-raster-using-gdal-bindings-in-python
        val proj = new SpatialReference(tile.getRaster.getRaster.GetProjection())
        Try(proj.AutoIdentifyEPSG())
        Try(proj.GetAttrValue("AUTHORITY", 1).toInt).getOrElse(0)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_SRID extends WithExpressionInfo {

    override def name: String = "rst_srid"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns SRID of the raster tile.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |        4326
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_SRID](1, expressionConfig)
    }

}
