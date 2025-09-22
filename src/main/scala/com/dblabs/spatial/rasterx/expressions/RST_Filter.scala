package com.dblabs.spatial.rasterx.expressions

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.rasterx.gdal.RasterDriver
import com.dblabs.spatial.rasterx.operations.KernelFilter
import com.dblabs.spatial.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset
import org.gdal.gdal.gdal

import scala.util.Try

/** The expression for applying NxN filter on a raster. */
case class RST_Filter(
    tileExpr: Expression,
    kernelSizeExpr: Expression,
    operationExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, kernelSizeExpr, operationExpr, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = "rst_filter"
    override def replacement: Expression = rstInvoke(RST_Filter, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Filter extends WithExpressionInfo {

    def evalPath(row: InternalRow, n: Int, operation: UTF8String, conf: UTF8String): InternalRow = eval(row, n, operation, conf, StringType)
    def evalBinary(row: InternalRow, n: Int, operation: UTF8String, conf: UTF8String): InternalRow =
        eval(row, n, operation, conf, BinaryType)

    def eval(row: InternalRow, n: Int, operation: UTF8String, conf: UTF8String, rdt: DataType): InternalRow = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val (cell, ds, _) = RasterSerializationUtil.rowToTile(row, rdt)
        val result = execute(ds, n, operation.toString)
        RasterDriver.releaseDataset(ds)
        val res = RasterSerializationUtil.tileToRow((cell, result._1, result._2), rdt, exprConf.hConf)
        RasterDriver.releaseDataset(result._1)
        res
    }

    def execute(ds: Dataset, n: Int, operation: String): (Dataset, Map[String, String]) = {
        val res = KernelFilter.filter(ds, n, operation)
        res
    }

    override def name: String = "rst_filter"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Filter(c(0), c(1), c(2))

}
