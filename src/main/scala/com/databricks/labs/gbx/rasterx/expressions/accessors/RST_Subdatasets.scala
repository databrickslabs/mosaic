package com.databricks.labs.gbx.rasterx.expressions.accessors

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.RasterAccessors
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Returns the subdatasets of the raster. */
case class RST_Subdatasets(
    tileExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def nullable: Boolean = true
    override def prettyName: String = RST_Subdatasets.name
    override def replacement: Expression = rstInvoke(RST_Subdatasets, rasterType)
    override def dataType: DataType = MapType(StringType, StringType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Subdatasets extends WithExpressionInfo {

    def evalPath(row: InternalRow, conf: UTF8String): MapData = eval(row, conf, StringType)
    def evalBinary(row: InternalRow, conf: UTF8String): MapData = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, rdt: DataType): MapData =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(row, rdt)
                val res = execute(ds)
                RasterDriver.releaseDataset(ds)
                SerializationUtil.toMapData[String, String](res)
            },
            row,
            rdt,
            conf
          )
        ).map(_.asInstanceOf[MapData]).orNull

    def execute(ds: Dataset): Map[String, String] = RasterAccessors.subdatasetsMap(ds)

    override def name: String = "gbx_rst_subdatasets"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Subdatasets(c(0))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        """Returns the subdatasets of the raster tile as a set of paths in the standard GDAL format.
          |The result is a map of the subdataset path to the subdatasets and the description of the subdatasets.""".stripMargin

    override def usageArgs: String = "tile"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(_ARGS_) FROM table;
           |      { "NETCDF:\"/dbfs/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa
           |      _max_7d_v3_1_20220106-1.nc\":bleaching_alert_area": "[1x3600x7200] N/A (8-bit unsigned integer)"
           |      , "NETCDF:\"/dbfs/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_m
           |      ax_7d_v3_1_20220106-1.nc\":mask": "[1x3600x7200] mask (8-bit unsigned integer)"}
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}"

}
