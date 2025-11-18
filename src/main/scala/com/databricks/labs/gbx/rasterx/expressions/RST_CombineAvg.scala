package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.CombineAVG
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Expression for combining rasters using average of pixels. */
case class RST_CombineAvg(
    tileExpr: Expression
) extends InvokedExpression {

    private def rasterType = tileExpr.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fields(1).dataType
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(rasterType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_CombineAvg.name
    override def replacement: Expression = rstInvoke(RST_CombineAvg, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_CombineAvg extends WithExpressionInfo {

    def evalPath(row: ArrayData, conf: UTF8String): InternalRow = eval(row, conf, StringType)
    def evalBinary(row: ArrayData, conf: UTF8String): InternalRow = eval(row, conf, BinaryType)

    def eval(array: ArrayData, conf: UTF8String, dt: DataType): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val tiles = RasterSerializationUtil.arrayToTiles(array, dt)
              val (cellID, combinedRaster, mtd) = execute(tiles)
              tiles.foreach(t => RasterDriver.releaseDataset(t._2))
              val res = RasterSerializationUtil.tileToRow((cellID, combinedRaster, mtd), dt, exprConf.hConf)
              RasterDriver.releaseDataset(combinedRaster)
              res
          },
          array,
          dt
        )

    def execute(tiles: Seq[(Long, Dataset, Map[String, String])]): (Long, Dataset, Map[String, String]) = {
        val cellID = if (tiles.map(_._1).groupBy(identity).size == 1) tiles.head._1 else -1L
        val (combinedRaster, mtd) = CombineAVG.compute(tiles.map(_._2).toArray, tiles.head._3)
        (cellID, combinedRaster, mtd)
    }

    override def name: String = "gbx_rst_combineavg"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_CombineAvg(c(0))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String = "Combines a collection of raster tiles by averaging the pixel values."

    override def usageArgs: String = "tiles"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(array(tile1, tile2, tile3) AS tile FROM table;
           |      ${_TILE_RESULT_}
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_ARRAY_TYPE_}"

}
