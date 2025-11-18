package com.databricks.labs.gbx.rasterx.expressions.generators

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.{BalancedSubdivision, RasterAccessors}
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
  * Creates raster tiles from the input column.
  *   - spark config to turn checkpointing on for all functions in 0.4.2
  *   - this is the only function able to write raster to checkpoint (even if
  *     the spark config is set to false).
  *   - can be useful when you want to start from the configured checkpoint but
  *     work with binary payloads from there.
  * @param tileExpr
  *   The expression for the raster. If the raster is stored on disc, the path
  *   to the raster is provided. If the raster is stored in memory, the bytes of
  *   the raster are provided.
  * @param sizeInMBExpr
  *   The size of the tiles in MB. If set to -1, the file is loaded and returned
  *   as a single tile. If set to 0, the file is loaded and subdivided into
  *   tiles of size 64MB. If set to a positive value, the file is loaded and
  *   subdivided into tiles of the specified size. If the file is too big to fit
  *   in memory, it is subdivided into tiles of size 64MB.
  * @param exprConfExpr
  *   Additional arguments for the expression (expressionConfigs).
  */
case class RST_MakeTiles(
    tileExpr: Expression,
    sizeInMBExpr: Expression,
    exprConfExpr: Expression = ExpressionConfigExpr()
) extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def position: Boolean = false
    override def inline: Boolean = false
    override def elementSchema: StructType = StructType(Array(StructField("tile", dataType)))
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))
    override def children: scala.Seq[Expression] = Seq(tileExpr, sizeInMBExpr, exprConfExpr)

    /**
      * Loads a raster from a file and subdivides it into tiles of the specified
      * size (in MB).
      * @param input
      *   The input file path.
      * @return
      *   The tiles.
      */
    override def eval(input: InternalRow): IterableOnce[InternalRow] =
        RST_ErrorHandler.safeEval(
          () => {
              val conf = exprConfExpr.eval(input).asInstanceOf[UTF8String]
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)

              val rawTile = tileExpr.eval(input).asInstanceOf[InternalRow]
              val (cell, ds, mtd) = RasterSerializationUtil.rowToTile(rawTile, rasterType)

              val targetSize = sizeInMBExpr.eval(input).asInstanceOf[Int]
              val inputSize = RasterAccessors.memSize(ds)

              if (targetSize <= 0 && inputSize <= Integer.MAX_VALUE) {
                  // - no split required
                  RasterDriver.releaseDataset(ds)
                  Seq(rawTile).iterator
              } else {
                  // target size is > 0 and raster size > target size
                  val iterator = BalancedSubdivision.splitRasterIter(ds, mtd, targetSize)
                  RST_ExpressionUtil.addCleanupListener(iterator)
                  iterator
                      .map { case (ds, mtd) =>
                          // convert the dataset to a row}
                          val tileRow = RasterSerializationUtil
                              .tileToRow((cell, ds, mtd), rasterType, exprConf.hConf)
                          RasterDriver.releaseDataset(ds)
                          InternalRow.fromSeq(Seq(tileRow)) // Row wrapping in generator
                      }
              }
          },
          input,
          rasterType
        )

}

/** Expression info required for the expression registration for spark SQL. */
object RST_MakeTiles extends WithExpressionInfo {

    override def name: String = "gbx_rst_maketiles"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_MakeTiles(c(0), c(1))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        "Subdivide the raster into tiles of the given size in MB."

    override def usageArgs: String = "tile, size_in_mb"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(tile, 16) AS tile FROM table;
           |      ${_TILE_RESULT_}
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}, size_in_mb: Int"

}
