package com.databricks.labs.gbx.rasterx.expressions.generators

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.ReTile
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
  * Returns a set of new rasters with the specified tile size (tileWidth x
  * tileHeight).
  */
case class RST_ReTile(
    tileExpr: Expression,
    tileWidthExpr: Expression,
    tileHeightExpr: Expression,
    exprConfExpr: Expression = ExpressionConfigExpr()
) extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def position: Boolean = false
    override def inline: Boolean = false
    override def elementSchema: StructType = StructType(Array(StructField("tile", dataType)))
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2), nc(3))
    override def children: Seq[Expression] = Seq(tileExpr, tileWidthExpr, tileHeightExpr, exprConfExpr)

    override def eval(input: InternalRow): IterableOnce[InternalRow] =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromExpr(exprConfExpr)
              RST_ExpressionUtil.init(exprConf)
              val rawTile = tileExpr.eval(input).asInstanceOf[InternalRow]
              val (cell, ds, mtd) = RasterSerializationUtil.rowToTile(rawTile, rasterType)
              val tileWidth = tileWidthExpr.eval(input).asInstanceOf[Int]
              val tileHeight = tileHeightExpr.eval(input).asInstanceOf[Int]
              val iter = ReTile.reTileIter(ds, mtd, tileWidth, tileHeight)
              RST_ExpressionUtil.addCleanupListener(iter)
              iter.map { case (newTile, newMtd) =>
                  val resRow = RasterSerializationUtil.tileToRow((cell, newTile, newMtd), rasterType, exprConf.hConf)
                  RasterDriver.releaseDataset(newTile)
                  InternalRow.fromSeq(Seq(resRow)) // Row wrapping in generator
              }
          },
          input,
          rasterType
        )

}

/** Expression info required for the expression registration for spark SQL. */
object RST_ReTile extends WithExpressionInfo {

    override def name: String = "gbx_rst_retile"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_ReTile(c(0), c(1), c(2))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        "Retiles the raster tile to the given size. The result is a collection of new raster tiles."

    override def usageArgs: String = "tile, tile_width, tile_height"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(tile, 300, 300) AS tile FROM table;
           |      {index_id: ..., raster: [00 01 10 ... 00], parentPath: "...", driver: "GTiff" }
           |      {index_id: ..., raster: [00 03 10 ... 00], parentPath: "...", driver: "GTiff" }""".stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}, tile_width: Int, tile_height: Int"

}
