package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types._

/** Returns the rotation angle of the raster. */
case class RST_Rotation(raster: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_Rotation](raster, returnsRaster = false, expressionConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = DoubleType

    /** Returns the rotation angle of the raster. */
    override def rasterTransform(tile: MosaicRasterTile): Any = {
        val gt = tile.getRaster.getRaster.GetGeoTransform()
        // arctan of y_skew and x_scale
        math.atan(gt(4) / gt(1))
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Rotation extends WithExpressionInfo {

    override def name: String = "rst_rotation"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns the rotation angle of the raster tile with respect to equator.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |        11.2
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Rotation](1, expressionConfig)
    }

}
