package com.dblabs.spatial.rasterx.expressions.generators

import com.dblabs.spatial.expressions.{ExpressionConfig, ExpressionConfigExpr, WithExpressionInfo}
import com.dblabs.spatial.rasterx.gdal.RasterDriver
import com.dblabs.spatial.rasterx.operations.RasterTessellate
import com.dblabs.spatial.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
  * Returns a set of new rasters which are the result of the tessellation of the
  * input raster.
  */
case class RST_H3_Tessellate(
    tileExpr: Expression,
    resolutionExpr: Expression,
    exprConfExpr: Expression = ExpressionConfigExpr()
) extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def position: Boolean = false
    override def inline: Boolean = false
    override def elementSchema: StructType = StructType(Array(StructField("tile", dataType)))
    override def children: Seq[Expression] = Seq(tileExpr, resolutionExpr, exprConfExpr)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

    override def eval(input: InternalRow): IterableOnce[InternalRow] = {
        val conf = exprConfExpr.eval(input).asInstanceOf[UTF8String]
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val rawTile = tileExpr.eval(input).asInstanceOf[InternalRow]
        val resolution = resolutionExpr.eval(input).asInstanceOf[Int]
        val (_, ds, mtd) = RasterSerializationUtil.rowToTile(rawTile, rasterType)
        val iter = RasterTessellate.tessellateH3Iter(ds, mtd, resolution)
        RST_ExpressionUtil.addCleanupListener(iter)
        iter
            .map { case (newCell, resDs, resMtd) =>
                val tile = RasterSerializationUtil.tileToRow((newCell, resDs, resMtd), rasterType, exprConf.hConf)
                RasterDriver.releaseDataset(resDs)
                tile
            }

    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_H3_Tessellate extends WithExpressionInfo {

    override def name: String = "rst_h3_tessellate"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_H3_Tessellate(c(0), c(1))

}
