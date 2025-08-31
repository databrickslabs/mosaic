package com.dblabs.spatial.rasterx.expressions.accessors

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.rasterx.gdal.RasterDriver
import com.dblabs.spatial.rasterx.operations.RasterAccessors
import com.dblabs.spatial.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Returns the memory size of the raster in bytes. */
case class RST_MemSize(
    tileExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def dataType: DataType = LongType
    override def nullable: Boolean = true
    override def prettyName: String = "rst_memsize"
    override def replacement: Expression = rstInvoke(RST_MemSize, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_MemSize extends WithExpressionInfo {

    def evalPath(row: InternalRow, conf: UTF8String): Long = eval(row, conf, StringType)
    def evalBinary(row: InternalRow, conf: UTF8String): Long = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, rdt: DataType): Long = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val ds = RasterSerializationUtil.rowToDS(row, rdt)
        val res = execute(ds)
        RasterDriver.releaseDataset(ds)
        res
    }

    def execute(ds: Dataset): Long = RasterAccessors.memSize(ds)

    override def name: String = "rst_memsize"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_MemSize(c(0))


}
