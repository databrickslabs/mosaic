package com.dblabs.spatial.rasterx.expressions.accessors

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.rasterx.gdal.RasterDriver
import com.dblabs.spatial.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants.GA_ReadOnly

/** Returns the subdatasets of the raster. */
case class RST_GetSubdataset(
    tileExpr: Expression,
    subsetName: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, subsetName, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = "rst_getsubdataset"
    override def replacement: Expression = rstInvoke(RST_GetSubdataset, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_GetSubdataset extends WithExpressionInfo {

    def evalPath(row: InternalRow, subsetName: UTF8String, conf: UTF8String): InternalRow = eval(row, subsetName, conf, StringType)
    def evalBinary(row: InternalRow, subsetName: UTF8String, conf: UTF8String): InternalRow = eval(row, subsetName, conf, BinaryType)

    def eval(row: InternalRow, subsetName: UTF8String, conf: UTF8String, rdt: DataType): InternalRow = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val (cell, ds, mtd) = RasterSerializationUtil.rowToTile(row, rdt)
        val subdataset = execute(ds, subsetName.toString)
        val res = RasterSerializationUtil.tileToRow((cell, subdataset, mtd), rdt, exprConf.hConf)
        RasterDriver.releaseDataset(ds)
        RasterDriver.releaseDataset(subdataset)
        res
    }

    def execute(ds: Dataset, name: String): Dataset = {
        val path = ds.GetDescription
        val driver = ds.GetDriver.getShortName
        val subdatasetPath = s"$driver:$path:$name"
        val subdataset = gdal.Open(subdatasetPath, GA_ReadOnly)
        subdataset
    }

    override def name: String = "rst_getsubdataset"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_GetSubdataset(c(0), c(1))

}
