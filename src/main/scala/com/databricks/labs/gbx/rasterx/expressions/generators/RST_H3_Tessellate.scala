package com.databricks.labs.gbx.rasterx.expressions.generators

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.RasterTessellate
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
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

    override def eval(input: InternalRow): IterableOnce[InternalRow] =
        RST_ErrorHandler.safeEval(
          () => {
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
                      InternalRow.fromSeq(Seq(tile)) // Row wrapping in generator
                  }

          },
          input,
          rasterType
        )

}

/** Expression info required for the expression registration for spark SQL. */
object RST_H3_Tessellate extends WithExpressionInfo {

    override def name: String = "gbx_rst_h3_tessellate"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_H3_Tessellate(c(0), c(1))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        """Divides the raster tile into tessellating chips for the given resolution of the supported grid (H3, BNG, Custom).
          |The result is a collection of new raster tiles.
          |Each tile in the tile set corresponds to an index cell intersecting the bounding box of the tile.""".stripMargin

    override def usageArgs: String = "tile, resolution"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(tile, 10) AS tile FROM table;
           |      {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "...", driver: "GTiff" }
           |      {index_id: 593308294097928192, raster: [00 03 10 ... 00], parentPath: "...", driver: "GTiff" }
           |
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}, resolution: Int"

}
