package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Returns the world coordinates of the raster (x,y) pixel. */
case class RST_RasterToWorldCoordX(
    tileExpr: Expression,
    x: Expression,
    y: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, x, y, ExpressionConfigExpr())
    override def dataType: DataType = DoubleType
    override def nullable: Boolean = true
    override def prettyName: String = RST_RasterToWorldCoordX.name
    override def replacement: Expression = rstInvoke(RST_RasterToWorldCoordX, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_RasterToWorldCoordX extends WithExpressionInfo {

    def evalPath(row: InternalRow, x: Int, y: Int, conf: UTF8String): Double = eval(row, x, y, conf, StringType)
    def evalBinary(row: InternalRow, x: Int, y: Int, conf: UTF8String): Double = eval(row, x, y, conf, BinaryType)

    def eval(row: InternalRow, x: Int, y: Int, conf: UTF8String, rdt: DataType): Double =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(row, rdt)
                val res = execute(ds, x, y)
                RasterDriver.releaseDataset(ds)
                res._1
            },
            row,
            rdt,
            conf
          )
        ).map(_.asInstanceOf[Double]).getOrElse(Double.NaN)

    def execute(ds: Dataset, x: Int, y: Int): (Double, Double) = GDAL.toWorldCoord(ds.GetGeoTransform, x, y)

    override def name: String = "gbx_rst_rastertoworldcoordx"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_RasterToWorldCoordX(c(0), c(1), c(2))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        """Computes the world coordinates of the raster tile at the given x and y pixel coordinates.
          |The result is the X coordinate of the point after applying the GeoTransform of the raster.""".stripMargin

    override def usageArgs: String = "tile, pixel_x, pixel_y"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(tile, 3, 3) AS tile FROM table;
           |       -179.85000609927647
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}, pixel_x: Int, pixel_y: Int"
}
