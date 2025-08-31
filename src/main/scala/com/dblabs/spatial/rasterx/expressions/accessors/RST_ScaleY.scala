package com.dblabs.spatial.rasterx.expressions.accessors

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.rasterx.gdal.RasterDriver
import com.dblabs.spatial.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Returns the scale x of the raster. */
case class RST_ScaleY(
    tileExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def dataType: DataType = DoubleType
    override def nullable: Boolean = true
    override def prettyName: String = "rst_scaley"
    override def replacement: Expression = rstInvoke(RST_ScaleY, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_ScaleY extends WithExpressionInfo {

    def evalPath(row: InternalRow, conf: UTF8String): Double = eval(row, conf, StringType)
    def evalBinary(row: InternalRow, conf: UTF8String): Double = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, rdt: DataType): Double = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val ds = RasterSerializationUtil.rowToDS(row, rdt)
        val res = execute(ds)
        RasterDriver.releaseDataset(ds)
        res
    }

    def execute(ds: Dataset): Double = {
        val gt = ds.GetGeoTransform
        // y_scale is the sixth element in the GeoTransform array
        gt(5)
    }

    override def name: String = "rst_scaley"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_ScaleY(c(0))


}
