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

/** Returns the pixel height of the raster. */
case class RST_PixelHeight(
    tileExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def dataType: DataType = DoubleType
    override def nullable: Boolean = true
    override def prettyName: String = RST_PixelHeight.name
    override def replacement: Expression = rstInvoke(RST_PixelHeight, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_PixelHeight extends WithExpressionInfo {

    def evalBinary(row: InternalRow, conf: UTF8String): Double = eval(row, conf, BinaryType)
    def evalPath(row: InternalRow, conf: UTF8String): Double = eval(row, conf, StringType)

    private def eval(row: InternalRow, conf: UTF8String, dt: DataType): Double =
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

    def execute(dataset: Dataset): Double = {
        val gt = dataset.GetGeoTransform
        val scaleY = gt(5)
        val skewX = gt(2)
        // when there is no skew the height is scaleY, but we cant assume 0-only skew
        // skew is not to be confused with rotation
        // TODO - check if this is correct
        math.sqrt(scaleY * scaleY + skewX * skewX)
    }

    override def name: String = "gbx_rst_pixelheight"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_PixelHeight(c(0))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        "Returns the height of the pixel in the raster tile derived via GeoTransform."

    override def usageArgs: String = "tile"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(_ARGS_) FROM table;
           |      1
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}"

}
