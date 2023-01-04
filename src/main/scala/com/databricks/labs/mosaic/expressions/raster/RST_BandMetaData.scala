package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.{MosaicRaster, MosaicRasterBand}
import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import com.databricks.labs.mosaic.expressions.base.{RasterBandExpression, WithExpressionInfo}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

case class RST_BandMetaData(inputRaster: Expression, band: Expression, path: Expression, rasterAPI: String)
    extends RasterBandExpression[RST_BandMetaData](inputRaster, band, path, MapType(StringType, StringType), RasterAPI(rasterAPI))
      with NullIntolerant
      with CodegenFallback {

    override def bandTransform(raster: MosaicRaster, band: MosaicRasterBand): Any = {
        val metaData = band.metadata
        buildMap(metaData)
    }
}

object RST_BandMetaData extends WithExpressionInfo {

    override def name: String = "rst_bandmetadata"

    override def usage: String = "_FUNC_(expr1, expr2, expr3) - Extracts metadata from a raster band."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, 1);
          |        {"NC_GLOBAL#acknowledgement":"NOAA Coral Reef Watch Program","NC_GLOBAL#cdm_data_type":"Grid"}
          |  """.stripMargin

    override def builder(args: Any*): FunctionBuilder = {
        val rasterAPI = args.headOption.getOrElse("GDAL").toString
        (children: Seq[Expression]) => {
            if (children.length != 3) {
                throw new IllegalArgumentException(s"$name function requires 3 arguments")
            }
            RST_BandMetaData(children(0), children(1), children(2), rasterAPI)
        }
    }

}
