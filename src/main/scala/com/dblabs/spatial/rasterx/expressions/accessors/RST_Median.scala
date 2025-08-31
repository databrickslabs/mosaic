package com.dblabs.spatial.rasterx.expressions.accessors

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.rasterx.gdal.{GDAL, RasterDriver}
import com.dblabs.spatial.rasterx.operator.GDALWarp
import com.dblabs.spatial.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, gdal}

/** Returns the median value per band of the raster. */
case class RST_Median(
    tileExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def dataType: DataType = ArrayType(DoubleType)
    override def nullable: Boolean = true
    override def prettyName: String = "rst_median"
    override def replacement: Expression = rstInvoke(RST_Median, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Median extends WithExpressionInfo {

    def evalPath(row: InternalRow, conf: UTF8String): ArrayData = eval(row, conf, StringType)
    def evalBinary(row: InternalRow, conf: UTF8String): ArrayData = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, rdt: DataType): ArrayData = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val ds = RasterSerializationUtil.rowToDS(row, rdt)
        val res = execute(ds, Map.empty)
        RasterDriver.releaseDataset(ds)
        ArrayData.toArrayData(res)
    }

    def execute(ds: Dataset, options: Map[String, String]): Array[Double] = {
        val outShortName = ds.GetDriver().getShortName
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val extension = GDAL.getExtension(outShortName)
        val resultPath = s"/vsimem/rst_median_$uuid.$extension"
        val cmd = s"gdalwarp -r med -ts 1 1"
        val (resDs, _) = GDALWarp.executeWarp(resultPath, Array(ds), options, cmd)
        val maxValues = (1 to resDs.GetRasterCount()).map(i => resDs.GetRasterBand(i).AsMDArray().GetStatistics().getMax)
        resDs.delete()
        gdal.Unlink(resultPath)
        maxValues.toArray
    }

    override def name: String = "rst_median"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Median(c(0))


}
