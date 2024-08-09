package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types._

/** Returns the rotation angle of the tile. */
case class RST_Rotation(raster: Expression, exprConfig: ExprConfig)
    extends RasterExpression[RST_Rotation](raster, returnsRaster = false, exprConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = DoubleType

    /** Returns the rotation angle of the tile. */
    override def rasterTransform(tile: RasterTile): Any = {
        tile.raster.getGeoTransformOpt match {
            case Some(gt) =>
                // arctan of y_skew and x_scale
                math.atan (gt (4) / gt (1) )
            case _ => 0d // double
        }
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Rotation extends WithExpressionInfo {

    override def name: String = "rst_rotation"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns the rotation angle of the tile tile with respect to equator.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |        11.2
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Rotation](1, exprConfig)
    }

}
