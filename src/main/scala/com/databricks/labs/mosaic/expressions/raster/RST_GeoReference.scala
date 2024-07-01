package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types._

/** Returns the georeference of the raster. */
case class RST_GeoReference(raster: Expression, exprConfig: ExprConfig)
    extends RasterExpression[RST_GeoReference](raster, returnsRaster = false, exprConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = MapType(StringType, DoubleType)

    /** Returns the georeference of the raster. */
    override def rasterTransform(tile: RasterTile): Any = {
        tile.raster.getGeoTransformOpt match {
            case Some(gt) =>
                buildMapDouble (
                    Map (
                        "upperLeftX" -> gt(0),
                        "upperLeftY" -> gt(3),
                        "scaleX" -> gt(1),
                        "scaleY" -> gt(5),
                        "skewX" -> gt(2),
                        "skewY" -> gt(4)
                    )
                )
            case _ => buildMapDouble(Map.empty[String, Double])
        }
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

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_GeoReference](1, exprConfig)
    }

}
