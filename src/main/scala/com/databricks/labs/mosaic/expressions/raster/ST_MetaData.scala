package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.expressions.base.{RasterExpression, WithExpressionInfo}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._

case class ST_MetaData(inputRaster: Expression, path: Expression)
    extends RasterExpression[ST_MetaData](inputRaster, path, MapType(StringType, StringType))
      with NullIntolerant
      with CodegenFallback {

    override def rasterTransform(raster: MosaicRaster): Any = {
        val metaData = raster.metadata
        buildMap(metaData)
    }
}

object ST_MetaData extends WithExpressionInfo {

    override def name: String = "st_metadata"

    override def usage: String = "_FUNC_(expr1) - Extracts metadata from a raster dataset."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |        {"NC_GLOBAL#acknowledgement":"NOAA Coral Reef Watch Program","NC_GLOBAL#cdm_data_type":"Grid"}
          |  """.stripMargin

    override def builder: FunctionBuilder =
        (exprs: Seq[Expression]) =>
            exprs match {
                case e if e.length == 1 => ST_MetaData(exprs(0), lit("").expr)
                case e if e.length == 2 => ST_MetaData(exprs(0), exprs(1))
            }

}
