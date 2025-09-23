package com.dblabs.gbx.rasterx.expressions

import com.dblabs.gbx.expressions._
import com.dblabs.gbx.rasterx.gdal.RasterDriver
import com.dblabs.gbx.rasterx.operations.TranslateFormat
import com.dblabs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, gdal}

import scala.util.Try

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

    private def eval(row: InternalRow, newFormat: UTF8String, conf: UTF8String, dt: DataType): InternalRow = {
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
    }

    def execute(ds: Dataset, mtd: Map[String, String], newFormat: String): (Dataset, Map[String, String]) = {
        if (ds.GetDriver().getShortName == newFormat) {
            (ds, mtd)
        } else {
            TranslateFormat.update(ds, mtd, newFormat)
        }
    }

    override def name: String = "gbx_rst_asformat"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_AsFormat(c(0), c(1))

}
