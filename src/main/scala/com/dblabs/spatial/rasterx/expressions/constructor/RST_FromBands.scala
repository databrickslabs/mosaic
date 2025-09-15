package com.dblabs.spatial.rasterx.expressions.constructor

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.rasterx.gdal.RasterDriver
import com.dblabs.spatial.rasterx.operations.MergeBands
import com.dblabs.spatial.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** The expression for stacking and resampling input bands. */
case class RST_FromBands(
    bandsExpr: Expression
) extends InvokedExpression {

    private def rasterType = bandsExpr.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fields(1).dataType
    override def children: Seq[Expression] = Seq(bandsExpr, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(rasterType)
    override def nullable: Boolean = true
    override def prettyName: String = "rst_frombands"
    override def replacement: Expression = rstInvoke(RST_FromBands, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_FromBands extends WithExpressionInfo {

    def evalBinary(row: ArrayData, conf: UTF8String): InternalRow = eval(row, conf, BinaryType)
    def evalPath(row: ArrayData, conf: UTF8String): InternalRow = eval(row, conf, StringType)

    def eval(row: ArrayData, conf: UTF8String, rdt: DataType): InternalRow = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val tiles = RasterSerializationUtil.arrayToTiles(row, rdt)
        val (ds, mtd) = execute(tiles)
        tiles.foreach(t => RasterDriver.releaseDataset(t._2))
        RasterSerializationUtil.tileToRow((tiles.head._1, ds, mtd), rdt, exprConf.hConf)
    }

    def execute(tiles: Seq[(Long, Dataset, Map[String, String])]): (Dataset, Map[String, String]) = {
        val rasters = tiles.map(_._2)
        val metadata = tiles.head._3
        MergeBands.merge(rasters, metadata, "bilinear")
    }

    override def name: String = "rst_frombands"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_FromBands(c(0))

}
