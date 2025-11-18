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

/** Returns the skew x of the raster. */
case class RST_SkewX(
    tileExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def dataType: DataType = DoubleType
    override def nullable: Boolean = true
    override def prettyName: String = RST_SkewX.name
    override def replacement: Expression = rstInvoke(RST_SkewX, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_SkewX extends WithExpressionInfo {

    def evalPath(row: InternalRow, conf: UTF8String): Double = eval(row, conf, StringType)
    def evalBinary(row: InternalRow, conf: UTF8String): Double = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, rdt: DataType): Double =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(row, rdt)
                val res = execute(ds)
                RasterDriver.releaseDataset(ds)
                res
            },
            row,
            rdt,
            conf
          )
        ).map(_.asInstanceOf[Double]).getOrElse(Double.NaN)

    def execute(ds: Dataset): Double = {
        val gt = ds.GetGeoTransform
        // x_skew is the third element in the GeoTransform array
        gt(2)
    }

    override def name: String = "gbx_rst_skewx"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_SkewX(c(0))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        "Computes the skew of the raster tile in the X direction."

    override def usageArgs: String = "tile"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(_ARGS_) FROM table;
           |      1.2
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}"

}
