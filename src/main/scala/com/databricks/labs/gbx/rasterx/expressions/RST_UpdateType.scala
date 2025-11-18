package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

case class RST_UpdateType(
    tileExpr: Expression,
    newType: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, newType, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = RST_UpdateType.name
    override def replacement: Expression = rstInvoke(RST_UpdateType, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_UpdateType extends WithExpressionInfo {

    def evalPath(row: InternalRow, newType: UTF8String, conf: UTF8String): InternalRow = eval(row, newType, conf, StringType)
    def evalBinary(row: InternalRow, newType: UTF8String, conf: UTF8String): InternalRow = eval(row, newType, conf, BinaryType)

    def eval(row: InternalRow, newType: UTF8String, conf: UTF8String, rdt: DataType): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, mtd) = RasterSerializationUtil.rowToTile(row, rdt)
              val result = execute(ds, mtd, newType.toString)
              RasterDriver.releaseDataset(ds)
              val res = RasterSerializationUtil.tileToRow((cell, result._1, result._2), rdt, exprConf.hConf)
              RasterDriver.releaseDataset(result._1)
              res
          },
          row,
          rdt
        )

    def execute(ds: Dataset, options: Map[String, String], newType: String): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val driver = ds.GetDriver()
        val extension = GDAL.getExtension(driver.getShortName)
        val resPath = s"/vsimem/$uuid.$extension"
        GDALTranslate.executeTranslate(
          resPath,
          ds,
          command = s"gdal_translate -ot $newType",
          options
        )
    }

    override def name: String = "gbx_rst_updatetype"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_UpdateType(c(0), c(1))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        "Translates the raster to a new data type."

    override def usageArgs: String = "tile, new_type"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(tile, 'Float32') AS tile FROM table;
           |      ${_TILE_RESULT_}
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}, new_type: String"
}
