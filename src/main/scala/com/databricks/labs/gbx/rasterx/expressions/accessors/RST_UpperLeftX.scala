package com.databricks.labs.gbx.rasterx.expressions.accessors

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Returns the upper left x of the raster. */
case class RST_UpperLeftX(
    tileExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def dataType: DataType = DoubleType
    override def nullable: Boolean = true
    override def prettyName: String = RST_UpperLeftX.name
    override def replacement: Expression = rstInvoke(RST_UpperLeftX, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_UpperLeftX extends WithExpressionInfo {

    def evalPath(row: InternalRow, conf: UTF8String): Double = eval(row, conf, StringType)
    def evalBinary(row: InternalRow, conf: UTF8String): Double = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, dt: DataType): Double =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(row, dt)
                val res = execute(ds)
                RasterDriver.releaseDataset(ds)
                res
            },
            row,
            dt,
            conf
          )
        ).map(_.asInstanceOf[Double]).getOrElse(Double.NaN)

    def execute(ds: Dataset): Double = {
        val gt = ds.GetGeoTransform
        // upper left x is the first element in the GeoTransform array
        gt(0)
    }

    override def name: String = "gbx_rst_upperleftx"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_UpperLeftX(c(0))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        "Computes the upper left X coordinate of tile based on its GeoTransform."

    override def usageArgs: String = "tile"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(_ARGS_) FROM table;
           |      -180.00000610436345
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}"

}
