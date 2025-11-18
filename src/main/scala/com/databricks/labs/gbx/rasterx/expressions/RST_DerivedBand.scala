package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.PixelCombineRasters
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Expression for combining rasters using average of pixels. */
case class RST_DerivedBand(
    tileExpr: Expression,
    pythonFuncExpr: Expression,
    funcNameExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, pythonFuncExpr, funcNameExpr, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = RST_DerivedBand.name
    override def replacement: Expression = rstInvoke(RST_DerivedBand, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_DerivedBand extends WithExpressionInfo {

    def evalPath(row: InternalRow, pyFunc: UTF8String, funcName: UTF8String, conf: UTF8String): InternalRow =
        eval(row, pyFunc, funcName, conf, StringType)
    def evalBinary(row: InternalRow, pyFunc: UTF8String, funcName: UTF8String, conf: UTF8String): InternalRow =
        eval(row, pyFunc, funcName, conf, BinaryType)

    def eval(row: InternalRow, pythonFunc: UTF8String, funcName: UTF8String, conf: UTF8String, rdt: DataType): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, mtd) = RasterSerializationUtil.rowToTile(row, rdt)
              val (newDs, newMtd) = execute(Seq(ds), mtd, pythonFunc.toString, funcName.toString)
              RasterDriver.releaseDataset(ds)
              val res = RasterSerializationUtil.tileToRow((cell, newDs, newMtd), rdt, exprConf.hConf)
              RasterDriver.releaseDataset(newDs)
              res
          },
          row,
          rdt
        )

    def execute(dss: Seq[Dataset], mtd: Map[String, String], pythonFunc: String, funcName: String): (Dataset, Map[String, String]) = {
        PixelCombineRasters.combine(dss.toArray, mtd, pythonFunc, funcName)
    }

    override def name: String = "gbx_rst_derivedband"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_DerivedBand(c(0), c(1), c(2))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String = "Combine tiles using the provided python function."

    override def usageArgs: String = "tile_expr, pyfunc, func_name"

    override def examples: String = {
        s"""
           |SELECT
           |_FUNC_(array(tile1,tile2,tile3), py_func1, func1_name) AS tile
           |FROM SELECT (
           |date, tile1, tile2, tile3,
           |\"\"\"
           |import numpy as np
           |def average(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
           |   out_ar[:] = np.sum(in_ar, axis=0) / len(in_ar)
           |\"\"\" as py_func1,
           |"average" as func1_name
           |FROM table
           |);
           |${_TILE_RESULT_}
           |""".stripMargin
    }

    override def extendedUsageArgs: String = s"tile_expr: <Raster Tile(s)> , pyfunc: String, func_name: String"
}
