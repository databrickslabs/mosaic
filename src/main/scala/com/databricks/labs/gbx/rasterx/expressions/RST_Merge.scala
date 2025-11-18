package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.MergeRasters
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Returns a raster that is a result of merging an array of rasters. */
case class RST_Merge(
    tileExpr: Expression
) extends InvokedExpression {

    private def rasterType = tileExpr.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fields(1).dataType
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(rasterType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Merge.name
    override def replacement: Expression = rstInvoke(RST_Merge, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Merge extends WithExpressionInfo {

    def evalPath(array: ArrayData, conf: UTF8String): InternalRow = eval(array, conf, StringType)
    def evalBinary(array: ArrayData, conf: UTF8String): InternalRow = eval(array, conf, BinaryType)

    def eval(array: ArrayData, conf: UTF8String, rdt: DataType): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val tiles = RasterSerializationUtil.arrayToTiles(array, rdt)
              val dss = tiles.map(_._2)
              val cell = tiles.head._1
              val (mergedDs, options) = execute(dss.toArray, tiles.head._3)
              dss.foreach(ds => RasterDriver.releaseDataset(ds))
              val res = RasterSerializationUtil.tileToRow((cell, mergedDs, options), rdt, exprConf.hConf)
              RasterDriver.releaseDataset(mergedDs)
              res
          },
          array,
          rdt
        )

    def execute(dss: Array[Dataset], options: Map[String, String]): (Dataset, Map[String, String]) = {
        MergeRasters.merge(dss, options)
    }

    override def name: String = "gbx_rst_merge"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Merge(c(0))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        "Combines a collection of raster tiles into a single raster."

    override def usageArgs: String = "tiles"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(array(tile1, tile2, tile3)) AS tile FROM table;
           |      ${_TILE_RESULT_}
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_ARRAY_TYPE_}"

}
