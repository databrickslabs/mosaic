package com.dblabs.spatial.rasterx.expressions.generators

import com.dblabs.spatial.expressions.{ExpressionConfig, ExpressionConfigExpr, WithExpressionInfo}
import com.dblabs.spatial.rasterx.operations.SeparateBands
import com.dblabs.spatial.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
  * Returns a set of new single-band rasters, one for each band in the input
  * raster.
  */
case class RST_SeparateBands(
    tileExpr: Expression,
    exprConfExpr: Expression = ExpressionConfigExpr()
) extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def position: Boolean = false
    override def inline: Boolean = false
    override def elementSchema: StructType = StructType(Array(StructField("tile", dataType)))
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), exprConfExpr)
    override def children: Seq[Expression] = Seq(tileExpr)

    override def eval(input: InternalRow): IterableOnce[InternalRow] = {
        val conf = exprConfExpr.eval(input).asInstanceOf[UTF8String]
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val rawTile = tileExpr.eval(input).asInstanceOf[InternalRow]
        val (cell, ds, mtd) = RasterSerializationUtil.rowToTile(rawTile, rasterType)
        SeparateBands.separateIter(ds, mtd).map { case (bandDs, bandMtd) =>
            RasterSerializationUtil.tileToRow((cell, bandDs, bandMtd), rasterType, exprConf.hConf)
        }
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_SeparateBands extends WithExpressionInfo {

    override def name: String = "rst_separatebands"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_SeparateBands(c(0))

}
