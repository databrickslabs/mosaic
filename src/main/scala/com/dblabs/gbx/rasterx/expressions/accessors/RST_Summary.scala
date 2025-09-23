package com.dblabs.gbx.rasterx.expressions.accessors

import com.dblabs.gbx.expressions._
import com.dblabs.gbx.rasterx.gdal.RasterDriver
import com.dblabs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.gdal.GDALInfo
import org.gdal.gdal.{Dataset, InfoOptions}

import java.util.{Vector => JVector}

/** Returns the summary info the raster. */
case class RST_Summary(
    tileExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def nullable: Boolean = true
    override def prettyName: String = RST_Summary.name
    override def replacement: Expression = rstInvoke(RST_Summary, rasterType)
    override def dataType: DataType = StringType
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Summary extends WithExpressionInfo {

    def evalPath(row: InternalRow, conf: UTF8String): UTF8String = eval(row, conf, StringType)
    def evalBinary(row: InternalRow, conf: UTF8String): UTF8String = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, rdt: DataType): UTF8String = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val ds = RasterSerializationUtil.rowToDS(row, rdt)
        val res = execute(ds)
        RasterDriver.releaseDataset(ds)
        UTF8String.fromString(res)
    }

    def execute(ds: Dataset): String = {
        val vector = new JVector[String]()
        vector.add("-json")
        val infoOptions = new InfoOptions(vector)
        val gdalInfo = GDALInfo(ds, infoOptions)
        gdalInfo
    }

    override def name: String = "gbx_rst_summary"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Summary(c(0))


}
