package com.dblabs.spatial.rasterx.expressions.accessors

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.rasterx.expressions.RST_DerivedBand
import com.dblabs.spatial.rasterx.gdal.RasterDriver
import com.dblabs.spatial.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import com.dblabs.spatial.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

import scala.jdk.CollectionConverters.DictionaryHasAsScala

/** Returns the metadata of the raster. */
case class RST_MetaData(
    tileExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def dataType: DataType = MapType(StringType, StringType)
    override def nullable: Boolean = true
    override def prettyName: String = "rst_metadata"
    override def replacement: Expression = rstInvoke(RST_DerivedBand, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_MetaData extends WithExpressionInfo {

    def evalPath(row: InternalRow, conf: UTF8String): MapData = eval(row, conf, StringType)
    def evalBinary(row: InternalRow, conf: UTF8String): MapData = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, rdt: DataType): MapData = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val ds = RasterSerializationUtil.rowToDS(row, rdt)
        val mtd = execute(ds)
        RasterDriver.releaseDataset(ds)
        SerializationUtil.toMapData[String, String](mtd)
    }

    def execute(ds: Dataset): Map[String, String] = {
        Option(ds.GetMetadataDomainList())
            .map(_.toArray)
            .map(domain =>
                domain
                    .map(domainName =>
                        Option(ds.GetMetadata_Dict(domainName.toString))
                            .map(_.asScala.toMap.asInstanceOf[Map[String, String]])
                            .getOrElse(Map.empty[String, String])
                    )
                    .reduceOption(_ ++ _)
                    .getOrElse(Map.empty[String, String])
            )
            .getOrElse(Map.empty[String, String])
    }

    override def name: String = "rst_metadata"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_MetaData(c(0))


}
