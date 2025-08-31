package com.dblabs.spatial.rasterx.expressions

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.rasterx.gdal.RasterDriver
import com.dblabs.spatial.rasterx.operations.NDVI
import com.dblabs.spatial.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** The expression for computing NDVI index. */
case class RST_NDVI(
    tileExpr: Expression,
    redIndex: Expression,
    nirIndex: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, redIndex, nirIndex, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = "rst_ndvi"
    override def replacement: Expression = rstInvoke(RST_NDVI, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_NDVI extends WithExpressionInfo {

    def evalPath(row: InternalRow, redIndex: Int, nirIndex: Int, conf: UTF8String): InternalRow =
        eval(row, redIndex, nirIndex, conf, StringType)
    def evalBinary(row: InternalRow, redIndex: Int, nirIndex: Int, conf: UTF8String): InternalRow =
        eval(row, redIndex, nirIndex, conf, BinaryType)

    def eval(row: InternalRow, redIndex: Int, nirIndex: Int, conf: UTF8String, dt: DataType): InternalRow = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val (cell, ds, mtd) = RasterSerializationUtil.rowToTile(row, dt)
        val (resultDs, resMtd) = execute(ds, redIndex, nirIndex, mtd)
        RasterDriver.releaseDataset(ds)
        RasterSerializationUtil.tileToRow((cell, resultDs, resMtd), dt, exprConf.hConf)
    }

    def execute(ds: Dataset, redIndex: Int, nirIndex: Int, options: Map[String, String]): (Dataset, Map[String, String]) = {
        NDVI.compute(ds, options, redIndex, nirIndex)
    }

    override def name: String = "rst_ndvi"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_NDVI(c(0), c(1), c(2))


}
