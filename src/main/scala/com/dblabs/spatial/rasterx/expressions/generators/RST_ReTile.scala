package com.dblabs.spatial.rasterx.expressions.generators

import com.dblabs.spatial.expressions.{ExpressionConfig, ExpressionConfigExpr, WithExpressionInfo}
import com.dblabs.spatial.rasterx.gdal.RasterDriver
import com.dblabs.spatial.rasterx.operations.ReTile
import com.dblabs.spatial.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

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

    override def eval(input: InternalRow): IterableOnce[InternalRow] = {
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
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_ReTile extends WithExpressionInfo {

    override def name: String = "rst_retile"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_ReTile(c(0), c(1), c(2))

}
