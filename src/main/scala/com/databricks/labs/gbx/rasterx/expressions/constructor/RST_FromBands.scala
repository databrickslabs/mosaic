package com.databricks.labs.gbx.rasterx.expressions.constructor

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.MergeBands
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
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
    override def prettyName: String = RST_FromBands.name
    override def replacement: Expression = rstInvoke(RST_FromBands, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_FromBands extends WithExpressionInfo {

    def evalBinary(row: ArrayData, conf: UTF8String): InternalRow = eval(row, conf, BinaryType)
    def evalPath(row: ArrayData, conf: UTF8String): InternalRow = eval(row, conf, StringType)

    def eval(row: ArrayData, conf: UTF8String, rdt: DataType): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val tiles = RasterSerializationUtil.arrayToTiles(row, rdt)
              val (ds, mtd) = execute(tiles)
              tiles.foreach(t => RasterDriver.releaseDataset(t._2))
              RasterSerializationUtil.tileToRow((tiles.head._1, ds, mtd), rdt, exprConf.hConf)
          },
          row,
          rdt
        )

    def execute(tiles: Seq[(Long, Dataset, Map[String, String])]): (Dataset, Map[String, String]) = {
        val rasters = tiles.map(_._2)
        val metadata = tiles.head._3
        MergeBands.merge(rasters, metadata, "bilinear")
    }

    override def name: String = "gbx_rst_frombands"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_FromBands(c(0))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        "Combines a collection of raster tiles of different bands into a single raster."

    override def usageArgs: String = "bands"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(array(tile1, tile2, tile3)) AS tile FROM table;
           |      ${_TILE_RESULT_}
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"bands: Array<Raster Tile>"

}
