package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.GDALWarp
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** The expression that initializes no data values of a raster. */
case class RST_InitNoData(
    tileExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = RST_InitNoData.name
    override def replacement: Expression = rstInvoke(RST_InitNoData, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_InitNoData extends WithExpressionInfo {

    def evalPath(row: InternalRow, conf: UTF8String): InternalRow = eval(row, conf, StringType)
    def evalBinary(row: InternalRow, conf: UTF8String): InternalRow = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, rdt: DataType): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, mdt) = RasterSerializationUtil.rowToTile(row, rdt)
              val (resultDs, newMdt) = execute(ds, mdt)
              RasterDriver.releaseDataset(ds)
              val res = RasterSerializationUtil.tileToRow((cell, resultDs, newMdt), rdt, exprConf.hConf)
              RasterDriver.releaseDataset(resultDs)
              res
          },
          row,
          rdt
        )

    def execute(ds: Dataset, options: Map[String, String]): (Dataset, Map[String, String]) = {
        val noDataValues = (1 to ds.GetRasterCount())
            .map { bandIndex =>
                val band = ds.GetRasterBand(bandIndex)
                GDAL.getNoDataConstant(band.getDataType)
            }
            .mkString(" ")
        val cmd = s"""gdalwarp -dstnodata "$noDataValues""""
        val uuid = java.util.UUID.randomUUID().toString
        val driver = ds.GetDriver()
        val extension = GDAL.getExtension(driver.getShortName)
        val resFile = s"/vsimem/initnodata_$uuid.$extension"
        GDALWarp.executeWarp(
          resFile,
          Array(ds),
          options,
          command = cmd
        )
    }

    override def name: String = "gbx_rst_initnodata"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_InitNoData(c(0))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        "Initializes the nodata value of the raster tile bands based on their data type."

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
