package com.dblabs.gbx.rasterx.expressions

import com.dblabs.gbx.expressions._
import com.dblabs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.dblabs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{BinaryType, DataType, IntegerType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Returns the x coordinate of the raster. */
case class RST_WorldToRasterCoordX(
    tileExpr: Expression,
    x: Expression,
    y: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, x, y, ExpressionConfigExpr())
    override def dataType: DataType = IntegerType
    override def nullable: Boolean = true
    override def prettyName: String = RST_WorldToRasterCoordX.name
    override def replacement: Expression = rstInvoke(RST_WorldToRasterCoordX, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_WorldToRasterCoordX extends WithExpressionInfo {

    def evalPath(row: InternalRow, xGeo: Double, yGeo: Double, conf: UTF8String): Int =
        eval(row, xGeo: Double, yGeo: Double, conf, StringType)
    def evalBinary(row: InternalRow, xGeo: Double, yGeo: Double, conf: UTF8String): Int =
        eval(row, xGeo: Double, yGeo: Double, conf, BinaryType)

    def eval(row: InternalRow, xGeo: Double, yGeo: Double, conf: UTF8String, dt: DataType): Int = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val ds = RasterSerializationUtil.rowToDS(row, dt)
        val res = execute(ds, xGeo, yGeo)
        RasterDriver.releaseDataset(ds)
        res
    }

    def execute(ds: Dataset, xGeo: Double, yGeo: Double): Int = GDAL.fromWorldCoord(ds.GetGeoTransform(), xGeo, yGeo)._1

    override def name: String = "gbx_rst_worldtorastercoordx"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_WorldToRasterCoordX(c(0), c(1), c(2))

}
