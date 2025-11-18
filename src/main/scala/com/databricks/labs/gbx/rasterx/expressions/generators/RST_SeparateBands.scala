package com.databricks.labs.gbx.rasterx.expressions.generators

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.SeparateBands
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

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
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))
    override def children: Seq[Expression] = Seq(tileExpr, exprConfExpr)

    override def eval(input: InternalRow): IterableOnce[InternalRow] =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromExpr(exprConfExpr)
              RST_ExpressionUtil.init(exprConf)
              val rawTile = tileExpr.eval(input).asInstanceOf[InternalRow]
              val (cell, ds, mtd) = RasterSerializationUtil.rowToTile(rawTile, rasterType)
              val iter = SeparateBands.separateIter(ds, mtd)
              RST_ExpressionUtil.addCleanupListener(iter)
              iter.map { case (bandDs, bandMtd) =>
                  val resRow = RasterSerializationUtil.tileToRow((cell, bandDs, bandMtd), rasterType, exprConf.hConf)
                  RasterDriver.releaseDataset(bandDs)
                  InternalRow.fromSeq(Seq(resRow)) // Row wrapping in generator
              }
          },
          input,
          rasterType
        )

}

/** Expression info required for the expression registration for spark SQL. */
object RST_SeparateBands extends WithExpressionInfo {

    override def name: String = "gbx_rst_separatebands"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_SeparateBands(c(0))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        """Returns a set of new single-band rasters, one for each band in the input raster.
          | The result set will contain one row per input band for each tile provided and will add field ‘bandIndex’.""".stripMargin

    override def usageArgs: String = "tile"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(_ARGS_) AS tile FROM table;
           |      ${_TILE_RESULT_}
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}"

}
