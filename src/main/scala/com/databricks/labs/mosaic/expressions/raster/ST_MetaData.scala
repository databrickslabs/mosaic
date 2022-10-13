package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapBuilder, ArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class ST_MetaData(inputRaster: Expression, path: Expression, rasterAPIName: String)
    extends BinaryExpression
      with NullIntolerant
      with CodegenFallback {

    val rasterAPI: RasterAPI = RasterAPI(rasterAPIName)

    override def left: Expression = inputRaster

    override def right: Expression = path

    override def dataType: DataType = MapType(keyType = StringType, valueType = StringType)

    override protected def nullSafeEval(rasterRow: Any, pathRow: Any): Any = {
        val path = pathRow.asInstanceOf[UTF8String].toString

        val baseRaster = rasterAPI.raster(rasterRow)
        val raster = rasterAPI.raster(rasterRow, path)
        val metaData = raster.metadata
        val result = buildMap(metaData)

        baseRaster.cleanUp()
        raster.cleanUp()
        result
    }

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
        copy(inputRaster = newLeft, path = newRight)

}

object ST_MetaData {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_MetaData].getCanonicalName,
          db.orNull,
          "st_metadata",
          """
            |    _FUNC_(expr1) - Extracts metadata from a raster dataset.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a);
            |        {"NC_GLOBAL#acknowledgement":"NOAA Coral Reef Watch Program","NC_GLOBAL#cdm_data_type":"Grid"}
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )

}
