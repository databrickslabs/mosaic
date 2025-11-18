package com.databricks.labs.gbx.rasterx.expressions.accessors

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

import scala.jdk.CollectionConverters.DictionaryHasAsScala

/** Returns the metadata of the raster. */
case class RST_MetaData(
    tileExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def dataType: DataType = MapType(StringType, StringType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_MetaData.name
    override def replacement: Expression = rstInvoke(RST_MetaData, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_MetaData extends WithExpressionInfo {

    def evalPath(row: InternalRow, conf: UTF8String): MapData = eval(row, conf, StringType)
    def evalBinary(row: InternalRow, conf: UTF8String): MapData = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, rdt: DataType): MapData =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(row, rdt)
                val mtd = execute(ds)
                RasterDriver.releaseDataset(ds)
                SerializationUtil.toMapData[String, String](mtd)
            },
            row,
            rdt,
            conf
          )
        ).map(_.asInstanceOf[MapData]).orNull

    def execute(ds: Dataset): Map[String, String] = {
        Option(ds.GetMetadataDomainList())
            .map(_.toArray)
            .map(domain =>
                domain
                    .map(domainName =>
                        Option(ds.GetMetadata_Dict(domainName.toString))
                            .map(_.asScala.toMap.asInstanceOf[Map[String, String]])
                            .getOrElse(Map.empty[String, String])
                    )
                    .reduceOption(_ ++ _)
                    .getOrElse(Map.empty[String, String])
            )
            .getOrElse(Map.empty[String, String])
    }

    override def name: String = "gbx_rst_metadata"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_MetaData(c(0))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        "Extract the metadata describing the raster tile. Metadata is return as a map of key value pairs."

    override def usageArgs: String = "tile"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(_ARGS_) FROM table;
           |      {"NC_GLOBAL#publisher_url": "https://coralreefwatch.noaa.gov", "NC_GLOBAL#geospatial_lat_units":
           |      "Degrees_north", "NC_GLOBAL#platform_vocabulary": "NOAA NODC Ocean Archive System Platforms",
           |      "NC_GLOBAL#creator_type": "group", "NC_GLOBAL#geospatial_lon_units": "degrees_east",
           |      "NC_GLOBAL#geospatial_bounds": "POLYGON((-90.0 180.0, 90.0 180.0, 90.0 -180.0, -90.0 -180.0,
           |      -90.0 180.0))", "NC_GLOBAL#keywords": "Oceans > Ocean Temperature > Sea Surface Temperature,
           |      Oceans > Ocean Temperature > Water Temperature, Spectral/Engineering > Infrared Wavelengths >
           |      Thermal Infrared, Oceans > Ocean Temperature > Bleaching Alert Area", "NC_GLOBAL#geospatial_lat_
           |      max": "89.974998", .... (truncated).... "NC_GLOBAL#history": "This is a product data file of the
           |      NOAA Coral Reef Watch Daily Global 5km Satellite Coral Bleaching Heat Stress Monitoring Product
           |      Suite Version 3.1 (v3.1) in its NetCDF Version 1.0 (v1.0).", "NC_GLOBAL#publisher_institution":
           |      "NOAA/NESDIS/STAR Coral Reef Watch Program", "NC_GLOBAL#cdm_data_type": "Grid"}
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}"

}
