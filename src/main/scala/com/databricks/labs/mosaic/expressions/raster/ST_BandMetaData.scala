package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, NullIntolerant, TernaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapBuilder, ArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class ST_BandMetaData(inputRaster: Expression, band: Expression, path: Expression, rasterAPIName: String)
    extends TernaryExpression
      with NullIntolerant
      with CodegenFallback {

    val rasterAPI: RasterAPI = RasterAPI(rasterAPIName)

    override def dataType: DataType = MapType(keyType = StringType, valueType = StringType)

    override def first: Expression = inputRaster

    override def second: Expression = band

    override def third: Expression = path

    override protected def nullSafeEval(rasterRow: Any, bandRow: Any, pathRow: Any): Any = {
        val path = pathRow.asInstanceOf[UTF8String].toString
        val bandIndex = bandRow.asInstanceOf[Int]

        val baseRaster = rasterAPI.raster(rasterRow)
        val raster = rasterAPI.raster(rasterRow, path)

        val metaData = raster.getBand(bandIndex).metadata
        val result = buildMap(metaData)

        baseRaster.cleanUp()
        raster.cleanUp()
        result
    }

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
