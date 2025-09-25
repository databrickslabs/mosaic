package com.dblabs.gbx.rasterx.expressions.accessors

import com.dblabs.gbx.expressions._
import com.dblabs.gbx.rasterx.gdal.RasterDriver
import com.dblabs.gbx.rasterx.operations.BandAccessors
import com.dblabs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Returns the max value per band of the raster. */
case class RST_Max(
    tileExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def dataType: DataType = ArrayType(DoubleType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Max.name
    override def replacement: Expression = rstInvoke(RST_Max, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Max extends WithExpressionInfo {

    def evalPath(row: InternalRow, conf: UTF8String): ArrayData = eval(row, conf, StringType)
    def evalBinary(row: InternalRow, conf: UTF8String): ArrayData = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, rdt: DataType): ArrayData = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val ds = RasterSerializationUtil.rowToDS(row, rdt)
        val res = execute(ds)
        RasterDriver.releaseDataset(ds)
        ArrayData.toArrayData(res)
    }

    def execute(ds: Dataset): Array[Double] = {
        (1 to ds.GetRasterCount()).map { bandIndex =>
            val band = ds.GetRasterBand(bandIndex)
            if (band == null) Double.NaN
            else {
                val (_, max) = BandAccessors.getMinMax(band)
                band.delete()
                max
            }
        }.toArray
    }

    override def name: String = "gbx_rst_max"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Max(c(0))

}
