package com.dblabs.spatial.rasterx.expressions

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.rasterx.gdal.{GDAL, RasterDriver}
import com.dblabs.spatial.rasterx.operator.GDALTranslate
import com.dblabs.spatial.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

case class RST_UpdateType(
    tileExpr: Expression,
    newType: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, newType, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = "rst_updatetype"
    override def replacement: Expression = rstInvoke(RST_UpdateType, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_UpdateType extends WithExpressionInfo {

    def evalPath(row: InternalRow, newType: UTF8String, conf: UTF8String): InternalRow = eval(row, newType, conf, StringType)
    def evalBinary(row: InternalRow, newType: UTF8String, conf: UTF8String): InternalRow = eval(row, newType, conf, BinaryType)

    def eval(row: InternalRow, newType: UTF8String, conf: UTF8String, rdt: DataType): InternalRow = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val (cell, ds, mtd) = RasterSerializationUtil.rowToTile(row, rdt)
        val res = execute(ds, mtd, newType.toString)
        RasterDriver.releaseDataset(ds)
        RasterSerializationUtil.tileToRow((cell, res._1, res._2), rdt, exprConf.hConf)
    }

    def execute(ds: Dataset, options: Map[String, String], newType: String): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val driver = ds.GetDriver()
        val extension = GDAL.getExtension(driver.getShortName)
        val resPath = s"/vsimem/$uuid.$extension"
        GDALTranslate.executeTranslate(
          resPath,
          ds,
          command = s"gdal_translate -ot $newType",
          options
        )
    }

    override def name: String = "rst_updatetype"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_UpdateType(c(0), c(1))


}
