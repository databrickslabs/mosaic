package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.TranslateFormat
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

case class RST_AsFormat(
    tileExpr: Expression,
    newFormat: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, newFormat, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = RST_AsFormat.name
    override def replacement: Expression = rstInvoke(RST_AsFormat, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_AsFormat extends WithExpressionInfo {

    def evalBinary(row: InternalRow, newFormat: UTF8String, conf: UTF8String): InternalRow = eval(row, newFormat, conf, BinaryType)
    def evalPath(row: InternalRow, newFormat: UTF8String, conf: UTF8String): InternalRow = eval(row, newFormat, conf, StringType)

    private def eval(row: InternalRow, newFormat: UTF8String, conf: UTF8String, dt: DataType): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, mtd) = RasterSerializationUtil.rowToTile(row, dt)
              if (ds.GetDriver().getShortName == newFormat.toString) {
                  RasterDriver.releaseDataset(ds)
                  row // effectively a no-op
              } else {
                  val (resDS, resMtd) = TranslateFormat.update(ds, mtd, newFormat.toString)
                  val res = RasterSerializationUtil.tileToRow((cell, resDS, resMtd), dt, exprConf.hConf)
                  RasterDriver.releaseDataset(resDS)
                  RasterDriver.releaseDataset(ds)
                  res
              }
          },
          row,
          dt
        )

    def execute(ds: Dataset, mtd: Map[String, String], newFormat: String): (Dataset, Map[String, String]) = {
        if (ds.GetDriver().getShortName == newFormat) {
            (ds, mtd)
        } else {
            TranslateFormat.update(ds, mtd, newFormat)
        }
    }

    override def name: String = "gbx_rst_asformat"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_AsFormat(c(0), c(1))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def usageArgs: String = "tile, new_format"

    override def description: String = "Convert a tile to a new format."

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}, new_format: String"

    override def examples: String = {
        s"""
           |# showing notional python
           |(
           | spark.read.format("gdal")
           |   .option("driverName", "netCDF")
           | .load("/Volumes/geospatial_docs/geobrix/data/netcdf/")
           |   .withColumn("tile", rx.rst_asformat("tile", f.lit("GTiff")))
           | .write.format("gdal")
           |   .mode("append")       # include "append" in the write
           |   .option("ext", "tif") # 'tif' (default)
           | .save("/Volumes/geospatial_docs/geobrix/data/out/netcdf-gtiff/")
           |)""".stripMargin
    }

    override def extendedDescription: String = "You may find that some formats are not configured or just don't work."

}
