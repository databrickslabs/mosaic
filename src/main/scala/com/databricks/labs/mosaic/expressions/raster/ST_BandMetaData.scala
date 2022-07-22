package com.databricks.labs.mosaic.expressions.raster

import org.apache.spark.sql.catalyst.expressions.{TernaryExpression, Expression, ExpressionInfo, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapBuilder, ArrayData}
import org.apache.spark.sql.types.{DataType, MapType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import com.databricks.labs.mosaic.core.raster.api.RasterAPI

case class ST_BandMetaData(inputRaster: Expression, band: Expression, path: Expression, rasterAPIName: String)
    extends TernaryExpression
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = MapType(keyType = StringType, valueType = StringType)

    private lazy val mapBuilder = new ArrayBasedMapBuilder(StringType, StringType)

    override protected def nullSafeEval(rasterRow: Any, bandRow: Any, pathRow: Any): Any = {
        val rasterAPI = RasterAPI(rasterAPIName)
        val raster = pathRow.asInstanceOf[UTF8String].toString match {
            case p: String if p == "" => rasterAPI.raster(rasterRow)
            case p: String            => rasterAPI.raster(rasterRow, p)
        }
        val bandIndex = bandRow.asInstanceOf[Int]
        val metaData = raster.getBand(bandIndex).metadata
        val keys = ArrayData.toArrayData(metaData.keys.toArray[String].map(UTF8String.fromString))
        val values = ArrayData.toArrayData(metaData.values.toArray[String].map(UTF8String.fromString))
        mapBuilder.putAll(keys, values)
        mapBuilder.build()
    }

    override def first: Expression = inputRaster

    override def second: Expression = band

    override def third: Expression = path

    override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression =
        copy(inputRaster = newFirst, band = newSecond, path = newThird)

}

object ST_BandMetaData {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_MetaData].getCanonicalName,
          db.orNull,
          "st_bandmetadata",
          """
            |    _FUNC_(expr1, expr2, expr3) - Extracts metadata from a raster band.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, 1);
            |        {"NC_GLOBAL#acknowledgement":"NOAA Coral Reef Watch Program","NC_GLOBAL#cdm_data_type":"Grid"}
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )

}
