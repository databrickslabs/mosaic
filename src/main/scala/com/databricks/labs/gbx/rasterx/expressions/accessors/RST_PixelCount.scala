package com.databricks.labs.gbx.rasterx.expressions.accessors

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Returns an array containing valid pixel count values for each band. */
case class RST_PixelCount(
    tileExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def dataType: DataType = ArrayType(LongType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_PixelCount.name
    override def replacement: Expression = rstInvoke(RST_PixelCount, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_PixelCount extends WithExpressionInfo {

    def evalBinary(row: InternalRow, conf: UTF8String): ArrayData = eval(row, conf, BinaryType)
    def evalPath(row: InternalRow, conf: UTF8String): ArrayData = eval(row, conf, StringType)

    private def eval(row: InternalRow, conf: UTF8String, dt: DataType): ArrayData =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(row, dt, shared = true) // ASMDArray requires shared dataset
                val tag = Integer.toHexString(System.identityHashCode(ds)) // unique tag for the dataset to avoid AsMDArray caching issues
                ds.SetDescription(s"${Option(ds.GetDescription).getOrElse("")}#mda-$tag")
                val counts = execute(ds)
                RasterDriver.releaseDataset(ds)
                ArrayData.toArrayData(counts)
            },
            row,
            dt,
            conf
          )
        ).map(_.asInstanceOf[ArrayData]).orNull

    def execute(ds: Dataset): Array[Long] = {
        (1 to ds.GetRasterCount())
            .map(i => {
                val band = ds.GetRasterBand(i)
                if (band == null) 0
                else {
                    val md = band.AsMDArray()
                    val stats = md.GetStatistics()
                    val res =
                        if (stats == null) 0
                        else stats.getValid_count
                    stats.delete()
                    md.delete()
                    band.delete()
                    res
                }
            })
            .toArray
    }

    override def name: String = "gbx_rst_pixelcount"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_PixelCount(c(0))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        "Returns an array containing valid pixel count values for each band."

    override def usageArgs: String = "tile"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(_ARGS_) FROM table;
           |      [120560172]
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}"

}
