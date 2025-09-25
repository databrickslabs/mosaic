package com.dblabs.gbx.rasterx.expressions.accessors

import com.dblabs.gbx.expressions._
import com.dblabs.gbx.rasterx.gdal.RasterDriver
import com.dblabs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset
import org.gdal.osr.SpatialReference

import scala.util.Try

/** Returns the SRID of the raster. */
case class RST_SRID(
    tileExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def dataType: DataType = IntegerType
    override def nullable: Boolean = true
    override def prettyName: String = RST_SRID.name
    override def replacement: Expression = rstInvoke(RST_SRID, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_SRID extends WithExpressionInfo {

    def evalPath(row: InternalRow, conf: UTF8String): Int = eval(row, conf, StringType)
    def evalBinary(row: InternalRow, conf: UTF8String): Int = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, rdt: DataType): Int = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val ds = RasterSerializationUtil.rowToDS(row, rdt)
        val res = execute(ds)
        RasterDriver.releaseDataset(ds)
        res
    }

    def execute(ds: Dataset): Int = {
        val proj = new SpatialReference(ds.GetProjection())
        Try(proj.AutoIdentifyEPSG())
        Try(proj.GetAttrValue("AUTHORITY", 1).toInt).getOrElse(0)
    }

    override def name: String = "gbx_rst_srid"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_SRID(c(0))

}
