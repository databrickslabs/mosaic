package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapBuilder, ArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.gdal

case class ST_Subdatasets(inputRaster: Expression, rasterAPIName: String) extends UnaryExpression with NullIntolerant with CodegenFallback {

    val rasterAPI: RasterAPI = RasterAPI(rasterAPIName)

    override def child: Expression = inputRaster

    override def dataType: DataType = MapType(keyType = StringType, valueType = StringType)

    override protected def nullSafeEval(rasterRow: Any): Any = {
        val raster = rasterAPI.raster(rasterRow)

        val metaData = raster.subdatasets
        val result = buildMap(metaData)

        raster.cleanUp()
        result
    }

    override protected def withNewChildInternal(newChild: Expression): Expression = copy(inputRaster = newChild)

}

object ST_Subdatasets {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_MetaData].getCanonicalName,
          db.orNull,
          "st_metadata",
          """
            |    _FUNC_(expr1) - Extracts subdataset paths and descriptions from a raster dataset.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a);
            |        {"NETCDF:"ct5km_baa-max-7d_v3.1_20220101.nc":bleaching_alert_area":"[1x3600x7200] N/A (8-bit unsigned integer)",
            |        "NETCDF:"ct5km_baa-max-7d_v3.1_20220101.nc":mask":"[1x3600x7200] mask (8-bit unsigned integer)"}
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )

}
