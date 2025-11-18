package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.util.Try

/** Returns true if the raster is empty. */
case class RST_TryOpen(
    tileExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def nullable: Boolean = true
    override def prettyName: String = RST_TryOpen.name
    override def replacement: Expression = rstInvoke(RST_TryOpen, rasterType)
    override def dataType: DataType = BooleanType
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_TryOpen extends WithExpressionInfo {

    def evalPath(row: InternalRow, conf: UTF8String): Boolean = eval(row, conf, StringType)
    def evalBinary(row: InternalRow, conf: UTF8String): Boolean = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, rdt: DataType): Boolean = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        Try {
            val ds = RasterSerializationUtil.rowToDS(row, rdt)
            RasterDriver.releaseDataset(ds)
            true
        }.toOption.isDefined
    }

    // execute not needed as in order to have an instance of ds the opening already succeeded
    // TODO: perhaps split into 2 executes String/Binary and avoid unified eval

    override def name: String = "gbx_rst_tryopen"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_TryOpen(c(0))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        """Tries to open the raster tile. If the raster cannot be opened the result is false,
          | and if the raster can be opened the result is true.""".stripMargin

    override def usageArgs: String = "tile"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(_ARGS_) FROM table;
           |      true
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}"
}
