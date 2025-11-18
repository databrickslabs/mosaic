package com.databricks.labs.gbx.rasterx.expressions.accessors

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.BandAccessors
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Band

/**
  * The expression for extracting metadata from a raster band.
  * @param tileExpr
  *   The expression for the raster. If the raster is stored on disk, the path
  *   to the raster is provided. If the raster is stored in memory, the bytes of
  *   the raster are provided.
  * @param band
  *   The band index.
  */
case class RST_BandMetaData(
    tileExpr: Expression,
    band: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, band, ExpressionConfigExpr())
    override def dataType: DataType = MapType(StringType, StringType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_BandMetaData.name
    override def replacement: Expression = rstInvoke(RST_BandMetaData, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_BandMetaData extends WithExpressionInfo {

    def evalPath(row: InternalRow, bandIndex: Int, conf: UTF8String): MapData = eval(row, bandIndex, conf, StringType)
    def evalBinary(row: InternalRow, bandIndex: Int, conf: UTF8String): MapData = eval(row, bandIndex, conf, BinaryType)

    def eval(row: InternalRow, bandIndex: Int, conf: UTF8String, dt: DataType): MapData =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(row, dt)
                val band = ds.GetRasterBand(bandIndex)
                val mtd = execute(band)
                band.delete()
                RasterDriver.releaseDataset(ds)
                SerializationUtil.toMapData[String, String](mtd)
            },
            row,
            dt,
            conf
          )
        ).map(_.asInstanceOf[MapData]).orNull

    def execute(band: Band): Map[String, String] = BandAccessors.getMetadata(band)

    override def name: String = "gbx_rst_bandmetadata"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_BandMetaData(c(0), c(1))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        "Extract the metadata describing the raster band. Metadata is returned as a map of key value pairs."

    override def usageArgs: String = "tile, band"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(tile, 1) AS tile FROM table;
           |      {"_FillValue": "251", "NETCDF_DIM_time": "1294315200", "long_name": "bleaching alert
           |      area 7-day maximum composite", "grid_mapping": "crs", "NETCDF_VARNAME":
           |      "bleaching_alert_area", "coverage_content_type": "thematicClassification",
           |      "standard_name": "N/A", "comment": "Bleaching Alert Area (BAA) values are coral
           |      bleaching heat stress levels: 0 - No Stress; 1 - Bleaching Watch; 2 - Bleaching
           |      Warning; 3 - Bleaching Alert Level 1; 4 - Bleaching Alert Level 2. Product
           |      description is provided at https://coralreefwatch.noaa.gov/product/5km/index.php.",
           |      "valid_min": "0", "units": "stress_level", "valid_max": "4", "scale_factor": "1"}
           |
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}, band: Int"

}
