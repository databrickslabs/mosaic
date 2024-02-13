package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types._

/** Returns the georeference of the raster. */
case class RST_GeoReference(raster: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_GeoReference](raster, returnsRaster = false, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = MapType(StringType, DoubleType)

    /** Returns the georeference of the raster. */
    override def rasterTransform(tile: MosaicRasterTile): Any = {
        val geoTransform = tile.getRaster.getRaster.GetGeoTransform()
        buildMapDouble(
          Map(
            "upperLeftX" -> geoTransform(0),
            "upperLeftY" -> geoTransform(3),
            "scaleX" -> geoTransform(1),
            "scaleY" -> geoTransform(5),
            "skewX" -> geoTransform(2),
            "skewY" -> geoTransform(4)
          )
        )
    }
}

/** Expression info required for the expression registration for spark SQL. */
object RST_GeoReference extends WithExpressionInfo {

    override def name: String = "rst_georeference"

    override def usage: String = "_FUNC_(expr1) - Extracts geo reference from a raster tile."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |        {"upper_left_x": 1.0, "upper_left_y": 1.0, "scale_x": 1.0, "scale_y": 1.0, "skew_x": 1.0, "skew_y": 1.0}
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_GeoReference](1, expressionConfig)
    }

}
