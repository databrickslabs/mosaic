package com.databricks.labs.gbx.rasterx.expressions.accessors

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants.GA_ReadOnly

/** Returns the subdatasets of the raster. */
case class RST_GetSubdataset(
    tileExpr: Expression,
    subsetName: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, subsetName, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = RST_GetSubdataset.name
    override def replacement: Expression = rstInvoke(RST_GetSubdataset, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_GetSubdataset extends WithExpressionInfo {

    def evalPath(row: InternalRow, subsetName: UTF8String, conf: UTF8String): InternalRow = eval(row, subsetName, conf, StringType)
    def evalBinary(row: InternalRow, subsetName: UTF8String, conf: UTF8String): InternalRow = eval(row, subsetName, conf, BinaryType)

    def eval(row: InternalRow, subsetName: UTF8String, conf: UTF8String, rdt: DataType): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, mtd) = RasterSerializationUtil.rowToTile(row, rdt)
              val subdataset = execute(ds, subsetName.toString)
              val res = RasterSerializationUtil.tileToRow((cell, subdataset, mtd), rdt, exprConf.hConf)
              RasterDriver.releaseDataset(ds)
              RasterDriver.releaseDataset(subdataset)
              res
          },
          row,
          rdt
        )

    def execute(ds: Dataset, name: String): Dataset = {
        val path = ds.GetDescription
        val driver = ds.GetDriver.getShortName
        val subdatasetPath = s"$driver:$path:$name"
        val subdataset = gdal.Open(subdatasetPath, GA_ReadOnly)
        subdataset
    }

    override def name: String = "gbx_rst_getsubdataset"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_GetSubdataset(c(0), c(1))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        "Returns the subdataset of the raster tile with a given name."

    override def usageArgs: String = "tile, subset_name"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(tile, 'sst') AS tile FROM table;
           |      ${_TILE_RESULT_}
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}, subset_name: String"

}
