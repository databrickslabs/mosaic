package com.databricks.labs.gbx.rasterx.expressions.accessors

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.BandAccessors
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Returns the data type of the raster. */
case class RST_Type(
    tileExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def nullable: Boolean = true
    override def prettyName: String = RST_Type.name
    override def replacement: Expression = rstInvoke(RST_Type, rasterType)
    override def dataType: DataType = ArrayType(StringType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Type extends WithExpressionInfo {

    def evalPath(row: InternalRow, conf: UTF8String): ArrayData = eval(row, conf, StringType)
    def evalBinary(row: InternalRow, conf: UTF8String): ArrayData = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, rdt: DataType): ArrayData =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(row, rdt)
                val res = execute(ds)
                RasterDriver.releaseDataset(ds)
                ArrayData.toArrayData(res.map(UTF8String.fromString))
            },
            row,
            rdt,
            conf
          )
        ).map(_.asInstanceOf[ArrayData]).orNull

    def execute(ds: Dataset): Array[String] = {
        val bandN = ds.GetRasterCount
        (1 to bandN).map { i =>
            val band = ds.GetRasterBand(i)
            BandAccessors.dataTypeHuman(band)
        }.toArray
    }

    override def name: String = "gbx_rst_type"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Type(c(0))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        "Returns the data type of the raster's bands."

    override def usageArgs: String = "tile"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(_ARGS_) FROM table;
           |      [Int16]
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}"

}
