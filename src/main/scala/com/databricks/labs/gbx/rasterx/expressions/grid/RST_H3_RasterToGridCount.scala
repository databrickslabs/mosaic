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

/** Returns the number of cells in the raster. */
case class RST_H3_RasterToGridCount(
    tileExpr: Expression,
    resolution: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, resolution, ExpressionConfigExpr())
    override def dataType: DataType =
        ArrayType(ArrayType(StructType(Seq(StructField("cellID", LongType), StructField("measure", IntegerType)))))
    override def nullable: Boolean = true
    override def prettyName: String = RST_H3_RasterToGridCount.name
    override def replacement: Expression = rstInvoke(RST_H3_RasterToGridCount, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_H3_RasterToGridCount extends WithExpressionInfo {

    def evalPath(row: InternalRow, resolution: Int, conf: UTF8String): ArrayData = eval(row, resolution, conf, StringType)
    def evalBinary(row: InternalRow, resolution: Int, conf: UTF8String): ArrayData = eval(row, resolution, conf, BinaryType)

    def eval(row: InternalRow, resolution: Int, conf: UTF8String, rdt: DataType): ArrayData =
        RST_H3_RasterToGrid.eval[Int](row, resolution, conf, rdt, this.execute)

    def execute(ds: Dataset, resolution: Int): Array[Array[(Long, Int)]] = {
        val meanF = (values: ArrayBuffer[Double]) => values.length
        RST_H3_RasterToGrid.execute[Int](ds, resolution, meanF)
    }

    override def name: String = "gbx_rst_h3_rastertogridcount"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_H3_RasterToGridCount(c(0), c(1))


}
