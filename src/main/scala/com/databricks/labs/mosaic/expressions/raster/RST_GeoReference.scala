package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

/** Returns the georeference of the raster. */
case class RST_GeoReference(path: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_GeoReference](path, MapType(StringType, DoubleType), expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    /** Returns the georeference of the raster. */
    override def rasterTransform(raster: MosaicRaster): Any = {
        val geoTransform = raster.getRaster.GetGeoTransform()
        val geoReference = Map(
          "upperLeftX" -> geoTransform(0),
          "upperLeftY" -> geoTransform(3),
          "scaleX" -> geoTransform(1),
          "scaleY" -> geoTransform(5),
          "skewX" -> geoTransform(2),
          "skewY" -> geoTransform(4)
        )
        buildMapDouble(geoReference)
    }
}

/** Expression info required for the expression registration for spark SQL. */
object RST_GeoReference extends WithExpressionInfo {

    override def name: String = "rst_georeference"

    override def usage: String = "_FUNC_(expr1, expr2) - Extracts geo reference from a raster."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, 1);
          |        {"upper_left_x": 1.0, "upper_left_y": 1.0, "scale_x": 1.0, "scale_y": 1.0, "skew_x": 1.0, "skew_y": 1.0}
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_GeoReference](1, expressionConfig)
    }

}
