package com.dblabs.gbx.rasterx.expressions

import com.dblabs.gbx.expressions._
import com.dblabs.gbx.rasterx.gdal.RasterDriver
import com.dblabs.gbx.rasterx.operations.RasterProject
import com.dblabs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.osr.SpatialReference

import scala.util.Try

/** Returns the upper left x of the raster. */
case class RST_Transform(
    tileExpr: Expression,
    srid: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, srid, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Transform.name
    override def replacement: Expression = rstInvoke(RST_Transform, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Transform extends WithExpressionInfo {

    def evalBinary(row: InternalRow, srid: Int, conf: UTF8String): InternalRow = eval(row, srid, conf, BinaryType)
    def evalPath(row: InternalRow, srid: Int, conf: UTF8String): InternalRow = eval(row, srid, conf, StringType)

    def eval(row: InternalRow, srid: Int, conf: UTF8String, dt: DataType): InternalRow = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val (cell, ds, options) = RasterSerializationUtil.rowToTile(row, dt)
        val (resultDs, metadata) = execute(ds, options, srid)
        RasterDriver.releaseDataset(ds)
        val res = RasterSerializationUtil.tileToRow((cell, resultDs, metadata), dt, exprConf.hConf)
        RasterDriver.releaseDataset(resultDs)
        res
    }

    def execute(ds: Dataset, options: Map[String, String], srid: Int): (Dataset, Map[String, String]) = {
        val dstSR = new SpatialReference()
        dstSR.ImportFromEPSG(srid)
        RasterProject.project(ds, options, dstSR)
    }

    override def name: String = "gbx_rst_transform"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Transform(c(0), c(1))


}
