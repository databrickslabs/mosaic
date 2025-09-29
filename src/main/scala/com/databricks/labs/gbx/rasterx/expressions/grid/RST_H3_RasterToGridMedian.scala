package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.expressions.{ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.util.RST_ExpressionUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

import scala.collection.mutable.ArrayBuffer

/** Returns the median value of the raster. */
case class RST_H3_RasterToGridMedian(
    tileExpr: Expression,
    resolution: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, resolution, ExpressionConfigExpr())
    override def dataType: DataType =
        ArrayType(ArrayType(StructType(Seq(StructField("cellID", LongType), StructField("measure", DoubleType)))))
    override def nullable: Boolean = true
    override def prettyName: String = RST_H3_RasterToGridMedian.name
    override def replacement: Expression = rstInvoke(RST_H3_RasterToGridMedian, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_H3_RasterToGridMedian extends WithExpressionInfo {

    def evalPath(row: InternalRow, resolution: Int, conf: UTF8String): ArrayData = eval(row, resolution, conf, StringType)
    def evalBinary(row: InternalRow, resolution: Int, conf: UTF8String): ArrayData = eval(row, resolution, conf, BinaryType)

    def eval(row: InternalRow, resolution: Int, conf: UTF8String, rdt: DataType): ArrayData =
        RST_H3_RasterToGrid.eval[Double](row, resolution, conf, rdt, this.execute)

    def execute(ds: Dataset, resolution: Int): Array[Array[(Long, Double)]] = {
        val meanF = (values: ArrayBuffer[Double]) => {
            val sorted = values.sorted
            val mid = sorted.length / 2
            if (sorted.length % 2 == 0) (sorted(mid - 1) + sorted(mid)) / 2.0
            else sorted(mid)
        }
        RST_H3_RasterToGrid.execute(ds, resolution, meanF)
    }

    override def name: String = "gbx_rst_h3_rastertogridmedian"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_H3_RasterToGridMedian(c(0), c(1))

}
