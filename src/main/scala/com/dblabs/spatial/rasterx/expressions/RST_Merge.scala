package com.dblabs.spatial.rasterx.expressions

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.rasterx.gdal.RasterDriver
import com.dblabs.spatial.rasterx.operations.MergeRasters
import com.dblabs.spatial.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, StringType, StructType}
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
    override def prettyName: String = "rst_merge"
    override def replacement: Expression = rstInvoke(RST_Merge, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Merge extends WithExpressionInfo {

    def evalPath(array: ArrayData, conf: UTF8String): InternalRow = eval(array, conf, StringType)
    def evalBinary(array: ArrayData, conf: UTF8String): InternalRow = eval(array, conf, BinaryType)

    def eval(array: ArrayData, conf: UTF8String, rdt: DataType): InternalRow = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val tiles = RasterSerializationUtil.arrayToTiles(array, rdt)
        val dss = tiles.map(_._2)
        val cell = tiles.head._1
        val (mergedDs, options) = execute(dss.toArray, tiles.head._3)
        dss.foreach(ds => RasterDriver.releaseDataset(ds))
        RasterSerializationUtil.tileToRow((cell, mergedDs, options), rdt, exprConf.hConf)
    }

    def execute(dss: Array[Dataset], options: Map[String, String]): (Dataset, Map[String, String]) = {
        MergeRasters.merge(dss, options)
    }

    override def name: String = "rst_merge"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Merge(c(0))


}
