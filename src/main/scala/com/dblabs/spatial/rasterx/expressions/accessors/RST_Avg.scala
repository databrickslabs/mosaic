package com.dblabs.spatial.rasterx.expressions.accessors

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.rasterx.gdal.RasterDriver
import com.dblabs.spatial.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Returns the avg value per band of the raster. */
case class RST_Avg(
    tileExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def dataType: DataType = ArrayType(DoubleType)
    override def nullable: Boolean = true
    override def prettyName: String = "rst_avg"
    override def replacement: PrettyInvoke = rstInvoke(RST_Avg, rasterType)

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Avg extends WithExpressionInfo {

    def evalBinary(row: InternalRow, conf: UTF8String): ArrayData = eval(row, conf, BinaryType)
    def evalPath(row: InternalRow, conf: UTF8String): ArrayData = eval(row, conf, StringType)

    def eval(row: InternalRow, conf: UTF8String, dt: DataType): ArrayData = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val ds = RasterSerializationUtil.rowToDS(row, dt)
        val res = execute(ds)
        RasterDriver.releaseDataset(ds)
        ArrayData.toArrayData(res)
    }

    def execute(ds: Dataset): Array[Double] = {
        (1 to ds.GetRasterCount()).map { bandIndex =>
            val band = ds.GetRasterBand(bandIndex)
            if (band == null) Double.NaN
            else {
                val stats = band.AsMDArray().GetStatistics()
                if (stats == null) Double.NaN
                else stats.getMean
            }
        }.toArray
    }

    override def name: String = "rst_avg"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Avg(c(0))

}
