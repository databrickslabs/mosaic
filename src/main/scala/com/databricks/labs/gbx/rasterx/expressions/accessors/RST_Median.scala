package com.databricks.labs.gbx.rasterx.expressions.accessors

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.GDALWarp
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, gdal}

/** Returns the median value per band of the raster. */
case class RST_Median(
    tileExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def dataType: DataType = ArrayType(DoubleType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Median.name
    override def replacement: Expression = rstInvoke(RST_Median, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Median extends WithExpressionInfo {

    def evalPath(row: InternalRow, conf: UTF8String): ArrayData = eval(row, conf, StringType)
    def evalBinary(row: InternalRow, conf: UTF8String): ArrayData = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, rdt: DataType): ArrayData =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(row, rdt)
                val res = execute(ds, Map.empty)
                RasterDriver.releaseDataset(ds)
                ArrayData.toArrayData(res)
            },
            row,
            rdt,
            conf
          )
        ).map(_.asInstanceOf[ArrayData]).orNull

    def execute(ds: Dataset, options: Map[String, String]): Array[Double] = {
        val outShortName = ds.GetDriver().getShortName
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val extension = GDAL.getExtension(outShortName)
        val resultPath = s"/vsimem/rst_median_$uuid.$extension"
        val cmd = s"gdalwarp -r med -ts 1 1"
        val (resDs, _) = GDALWarp.executeWarp(resultPath, Array(ds), options, cmd)
        val maxValues = (1 to resDs.GetRasterCount()).map(i => resDs.GetRasterBand(i).AsMDArray().GetStatistics().getMax)
        resDs.delete()
        gdal.Unlink(resultPath)
        maxValues.toArray
    }

    override def name: String = "gbx_rst_median"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Median(c(0))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        "Returns an array containing median values for each band."

    override def usageArgs: String = "tile"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(_ARGS_) FROM table;
           |      [42.0]
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}"

}
