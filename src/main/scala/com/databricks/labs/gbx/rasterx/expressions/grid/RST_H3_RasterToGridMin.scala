package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.expressions.{ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

import scala.collection.mutable.ArrayBuffer

/** Returns the minimum value of the raster in the grid cell. */
case class RST_H3_RasterToGridMin(
    tileExpr: Expression,
    resolution: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, resolution, ExpressionConfigExpr())
    override def dataType: DataType =
        ArrayType(ArrayType(StructType(Seq(StructField("cellID", LongType), StructField("measure", DoubleType)))))
    override def nullable: Boolean = true
    override def prettyName: String = RST_H3_RasterToGridMin.name
    override def replacement: Expression = rstInvoke(RST_H3_RasterToGridMin, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_H3_RasterToGridMin extends WithExpressionInfo {

    def evalPath(row: InternalRow, resolution: Int, conf: UTF8String): ArrayData = eval(row, resolution, conf, StringType)
    def evalBinary(row: InternalRow, resolution: Int, conf: UTF8String): ArrayData = eval(row, resolution, conf, BinaryType)

    def eval(row: InternalRow, resolution: Int, conf: UTF8String, rdt: DataType): ArrayData =
        Option(RST_ErrorHandler.safeEval(() => RST_H3_RasterToGrid.eval[Double](row, resolution, conf, rdt, this.execute), row, rdt, conf))
            .map(_.asInstanceOf[ArrayData])
            .orNull

    def execute(ds: Dataset, resolution: Int): Array[Array[(Long, Double)]] = {
        val meanF = (values: ArrayBuffer[Double]) => values.min
        RST_H3_RasterToGrid.execute(ds, resolution, meanF)
    }

    override def name: String = "gbx_rst_h3_rastertogridmin"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_H3_RasterToGridMin(c(0), c(1))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        """Compute the gridwise min of the pixel values in tile.
          |The result is a 2D array of cells, where each cell is a struct of (cellID, value).
          |""".stripMargin

    override def usageArgs: String = "tile, resolution"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(tile, 3) FROM table;
           |      [[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure":
           |        1.2037735849056603}, {"cellID": "593308294097928191", "measure": 0}]]""".stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}, resolution: Int"

}
