package com.dblabs.gbx.rasterx.expressions.accessors

import com.dblabs.gbx.expressions._
import com.dblabs.gbx.rasterx.gdal.RasterDriver
import com.dblabs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import com.dblabs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Returns the georeference of the raster. */
case class RST_GeoReference(
    tileExpr: Expression,
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def dataType: DataType = MapType(StringType, DoubleType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_GeoReference.name
    override def replacement: Expression = rstInvoke(RST_GeoReference, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_GeoReference extends WithExpressionInfo {

    def evalPath(row: InternalRow, conf: UTF8String): MapData = eval(row, conf, StringType)
    def evalBinary(row: InternalRow, conf: UTF8String): MapData = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, rdt: DataType): MapData = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val ds = RasterSerializationUtil.rowToDS(row, rdt)
        val res = execute(ds)
        RasterDriver.releaseDataset(ds)
        SerializationUtil.toMapData[String, Double](res)
    }

    def execute(ds: Dataset): Map[String, Double] = {
        val geoTransform = ds.GetGeoTransform()
        Map(
          "upperLeftX" -> geoTransform(0),
          "upperLeftY" -> geoTransform(3),
          "scaleX" -> geoTransform(1),
          "scaleY" -> geoTransform(5),
          "skewX" -> geoTransform(2),
          "skewY" -> geoTransform(4)
        )
    }

    override def name: String = "gbx_rst_georeference"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_GeoReference(c(0))


}
