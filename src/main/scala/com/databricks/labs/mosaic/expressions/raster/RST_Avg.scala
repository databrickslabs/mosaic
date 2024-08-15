package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALInfo
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

import scala.util.Try


/** Returns the avg value per band of the tile. */
case class RST_Avg(tileExpr: Expression, exprConfig: ExprConfig)
    extends RasterExpression[RST_Avg](tileExpr, returnsRaster = false, exprConfig)
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = ArrayType(DoubleType)

    /** Returns the avg value per band of the tile. */
    override def rasterTransform(tile: RasterTile): Any =
        Try {
            import org.json4s._
            import org.json4s.jackson.JsonMethods._
            implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

            val command = s"gdalinfo -stats -json -mm -nogcp -nomd -norat -noct"
            val gdalInfo = GDALInfo.executeInfo(tile.raster, command)

            // parse json from gdalinfo
            // - can print out during debugging
            // - essentially if this doesn't parse
            //   then will throw an exception down below
            val json = Try(parse(gdalInfo).extract[Map[String, Any]]).getOrElse(null)

            // if the above failed, this block will throw an exception
            val meanValues = json("bands").asInstanceOf[List[Map[String, Any]]].map { band =>
                band("mean").asInstanceOf[Double]
            }

            ArrayData.toArrayData(meanValues.toArray)
        }.getOrElse(ArrayData.toArrayData(Array.empty[Double]))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Avg extends WithExpressionInfo {

    override def name: String = "rst_avg"

    override def usage: String = "_FUNC_(expr1) - Returns an array containing mean values for each band."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile);
          |       [1.123, 2.123, 3.123]
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Avg](1, exprConfig)
    }

}
