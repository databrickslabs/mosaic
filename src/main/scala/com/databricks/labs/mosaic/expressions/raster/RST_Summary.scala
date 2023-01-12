package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.gdal.GDALInfo
import org.gdal.gdal.InfoOptions

import java.util.{Vector => JVector}

/** Returns the summary info the raster. */
case class RST_Summary(path: Expression, expressionConfig: MosaicExpressionConfig)
    extends RasterExpression[RST_Summary](path, StringType, expressionConfig: MosaicExpressionConfig)
      with NullIntolerant
      with CodegenFallback {

    /** Returns the summary info the raster. */
    override def rasterTransform(raster: MosaicRaster): Any = {
        val vector = new JVector[String]()
        // For other flags check the way gdalinfo.py script is called, InfoOptions expects a collection of same flags.
        // https://gdal.org/programs/gdalinfo.html
        vector.add("-json")
        val infoOptions = new InfoOptions(vector)
        val gdalInfo = GDALInfo(raster.getRaster, infoOptions)
        UTF8String.fromString(gdalInfo)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Summary extends WithExpressionInfo {

    override def name: String = "rst_summary"

    override def usage: String = "_FUNC_(expr1) - Generates GDAL summary for the raster."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a);
          |        {
          |             "description":"byte.tif",
          |             "driverShortName":"GTiff",
          |             "driverLongName":"GeoTIFF",
          |             "files":[
          |                 "byte.tif"
          |             ],
          |         ....
          |        }
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_Summary](1, expressionConfig)
    }

}
