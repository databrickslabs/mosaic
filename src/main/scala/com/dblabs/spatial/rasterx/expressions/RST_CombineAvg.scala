package com.dblabs.spatial.rasterx.expressions

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.rasterx.gdal.RasterDriver
import com.dblabs.spatial.rasterx.operations.CombineAVG
import com.dblabs.spatial.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Expression for combining rasters using average of pixels. */
case class RST_CombineAvg(
    tileExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = "rst_combineavg"
    override def replacement: Expression = rstInvoke(RST_CombineAvg, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_CombineAvg extends WithExpressionInfo {

    def eval(row: InternalRow, conf: UTF8String, dt: DataType): InternalRow = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val tilesRaw = row.getArray(0)
        val tiles = (0 until tilesRaw.numElements()).map { i =>
            val tile = tilesRaw.getStruct(i, 3)
            RasterSerializationUtil.rowToTile(tile, dt)
        }
        val (cellID, combinedRaster, mtd) = execute(tiles)
        tiles.foreach(t => RasterDriver.releaseDataset(t._2))
        RasterSerializationUtil.tileToRow((cellID, combinedRaster, mtd), dt, exprConf.hConf)
    }

    def execute(tiles: Seq[(Long, Dataset, Map[String, String])]): (Long, Dataset, Map[String, String]) = {
        val cellID = if (tiles.map(_._1).groupBy(identity).size == 1) tiles.head._1 else -1L
        val (combinedRaster, mtd) = CombineAVG.compute(tiles.map(_._2).toArray, tiles.head._3)
        (cellID, combinedRaster, mtd)
    }

    override def name: String = "rst_combineavg"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_CombineAvg(c(0))


}
