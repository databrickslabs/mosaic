package com.databricks.labs.mosaic.expressions.raster

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapBuilder, ArrayData}
import org.apache.spark.sql.types.{DataType, MapType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import com.databricks.labs.mosaic.core.raster.api.RasterAPI

case class ST_MetaData(inputRaster: Expression, path: Expression, rasterAPIName: String)
    extends BinaryExpression
      with NullIntolerant
      with CodegenFallback {

    override def left: Expression = inputRaster

    override def right: Expression = path

    override def dataType: DataType = MapType(keyType = StringType, valueType = StringType)

    private lazy val mapBuilder = new ArrayBasedMapBuilder(StringType, StringType)

    override protected def nullSafeEval(rasterRow: Any, pathRow: Any): Any = {
        val rasterAPI = RasterAPI(rasterAPIName)
        val raster = pathRow.asInstanceOf[UTF8String].toString match {
            case p: String if p == "" => rasterAPI.raster(rasterRow)
            case p: String            => rasterAPI.raster(rasterRow, p)
        }
        val metaData = raster.metadata
        val keys = ArrayData.toArrayData(metaData.keys.toArray[String].map(UTF8String.fromString))
        val values = ArrayData.toArrayData(metaData.values.toArray[String].map(UTF8String.fromString))
        mapBuilder.putAll(keys, values)
        mapBuilder.build()
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
