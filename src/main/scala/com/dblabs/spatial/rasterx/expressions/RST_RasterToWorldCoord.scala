package com.dblabs.spatial.rasterx.expressions

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.rasterx.gdal.{GDAL, RasterDriver}
import com.dblabs.spatial.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Returns the world coordinates of the raster (x,y) pixel. */
case class RST_RasterToWorldCoord(
    tileExpr: Expression,
    x: Expression,
    y: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, x, y, ExpressionConfigExpr())
    override def dataType: DataType =
        StructType(
          Seq(
            StructField("x", DoubleType, nullable = true),
            StructField("y", DoubleType, nullable = true)
          )
        )
    override def nullable: Boolean = true
    override def prettyName: String = "rst_rastertoworldcoord"
    override def replacement: Expression = rstInvoke(RST_RasterToWorldCoord, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_RasterToWorldCoord extends WithExpressionInfo {

    def evalPath(row: InternalRow, x: Int, y: Int, conf: UTF8String): InternalRow = eval(row, x, y, conf, StringType)
    def evalBinary(row: InternalRow, x: Int, y: Int, conf: UTF8String): InternalRow = eval(row, x, y, conf, BinaryType)

    def eval(row: InternalRow, x: Int, y: Int, conf: UTF8String, rdt: DataType): InternalRow = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val ds = RasterSerializationUtil.rowToDS(row, rdt)
        val res = execute(ds, x, y)
        RasterDriver.releaseDataset(ds)
        InternalRow.fromSeq(Seq(res._1, res._2))
    }

    def execute(ds: Dataset, x: Int, y: Int): (Double, Double) = GDAL.toWorldCoord(ds.GetGeoTransform, x, y)

    override def name: String = "rst_rastertoworldcoord"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_RasterToWorldCoord(c(0), c(1), c(2))


}
