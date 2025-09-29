package com.databricks.labs.gbx.rasterx.expressions.accessors

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

case class RST_Format(
    tileExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def dataType: DataType = StringType
    override def nullable: Boolean = true
    override def prettyName: String = RST_Format.name
    override def replacement: Expression = rstInvoke(RST_Format, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Format extends WithExpressionInfo {

    def evalBinary(row: InternalRow, conf: UTF8String): UTF8String = eval(row, conf, BinaryType)
    def evalPath(row: InternalRow, conf: UTF8String): UTF8String = eval(row, conf, StringType)

    private def eval(row: InternalRow, conf: UTF8String, dt: DataType): UTF8String = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val ds = RasterSerializationUtil.rowToDS(row, dt)
        val res = execute(ds)
        RasterDriver.releaseDataset(ds)
        UTF8String.fromString(res)
    }

    def execute(ds: Dataset): String = {
        ds.GetDriver.getShortName
    }

    override def name: String = "gbx_rst_format"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Format(c(0))


}
