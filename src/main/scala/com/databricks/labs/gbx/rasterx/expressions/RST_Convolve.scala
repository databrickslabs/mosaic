package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.Convolve
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** The expression for applying kernel filter on a raster. */
case class RST_Convolve(
    tileExpr: Expression,
    kernelExpr: Expression
) extends InvokedExpression {

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))
    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    private def kernelType = kernelExpr.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[ArrayType].elementType
    override def children: Seq[Expression] = Seq(tileExpr, kernelExpr, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Convolve.name
    override def replacement: Expression =
        (rasterType, kernelType) match {
            case (StringType, DoubleType)  => invoke(RST_Convolve, "evalPathDouble")
            case (BinaryType, DoubleType)  => invoke(RST_Convolve, "evalBinaryDouble")
            case (StringType, IntegerType) => invoke(RST_Convolve, "evalPathInt")
            case (BinaryType, IntegerType) => invoke(RST_Convolve, "evalBinaryInt")
            case (StringType, FloatType)   => invoke(RST_Convolve, "evalPathFloat")
            case (BinaryType, FloatType)   => invoke(RST_Convolve, "evalBinaryFloat")
            case (StringType, LongType)    => invoke(RST_Convolve, "evalPathLong")
            case (BinaryType, LongType)    => invoke(RST_Convolve, "evalBinaryLong")
        }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Convolve extends WithExpressionInfo {

    def evalPathDouble(row: InternalRow, kernelAD: ArrayData, conf: UTF8String): InternalRow = eval(row, kernelAD, conf, StringType, DoubleType)
    def evalBinaryDouble(row: InternalRow, kernelAD: ArrayData, conf: UTF8String): InternalRow = eval(row, kernelAD, conf, BinaryType, DoubleType)
    def evalPathInt(row: InternalRow, kernelAD: ArrayData, conf: UTF8String): InternalRow = eval(row, kernelAD, conf, StringType, IntegerType)
    def evalBinaryInt(row: InternalRow, kernelAD: ArrayData, conf: UTF8String): InternalRow = eval(row, kernelAD, conf, BinaryType, IntegerType)
    def evalPathFloat(row: InternalRow, kernelAD: ArrayData, conf: UTF8String): InternalRow = eval(row, kernelAD, conf, StringType, FloatType)
    def evalBinaryFloat(row: InternalRow, kernelAD: ArrayData, conf: UTF8String): InternalRow = eval(row, kernelAD, conf, BinaryType, FloatType)
    def evalPathLong(row: InternalRow, kernelAD: ArrayData, conf: UTF8String): InternalRow = eval(row, kernelAD, conf, StringType, LongType)
    def evalBinaryLong(row: InternalRow, kernelAD: ArrayData, conf: UTF8String): InternalRow = eval(row, kernelAD, conf, BinaryType, LongType)

    def eval(row: InternalRow, kernelAD: ArrayData, conf: UTF8String, rdt: DataType, kdt: DataType): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val tile = RasterSerializationUtil.rowToTile(row, rdt)
              val kernel = kdt match {
                  case DoubleType  => SerializationUtil.create2DArray[Double](kernelAD, kdt)
                  case IntegerType => SerializationUtil.create2DArray[Int](kernelAD, kdt).map(_.map(_.toDouble))
                  case FloatType   => SerializationUtil.create2DArray[Float](kernelAD, kdt).map(_.map(_.toDouble))
                  case LongType    => SerializationUtil.create2DArray[Long](kernelAD, kdt).map(_.map(_.toDouble))
              }
              val (raster, metadata) = Convolve.convolve(tile._2, tile._3, kernel)
              RasterDriver.releaseDataset(tile._2)
              val res = RasterSerializationUtil.tileToRow((tile._1, raster, metadata), rdt, exprConf.hConf)
              RasterDriver.releaseDataset(raster)
              res
          },
          row,
          rdt
        )

    def execute(tile: (Long, Dataset, Map[String, String]), kernel: Array[Array[Double]]): (Dataset, Map[String, String]) = {
        Convolve.convolve(tile._2, tile._3, kernel)
    }

    override def name: String = "gbx_rst_convolve"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Convolve(c(0), c(1))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String = "Applies a convolution filter to the raster."

    override def usageArgs: String = "tile, kernel"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(_ARGS_) AS tile FROM table;
           |      ${_TILE_RESULT_}
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}, kernel: Array<Array<Double>>>"

}
