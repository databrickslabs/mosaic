package com.dblabs.spatial.rasterx.expressions.accessors

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.rasterx.gdal.RasterDriver
import com.dblabs.spatial.rasterx.operations.BandAccessors
import com.dblabs.spatial.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import com.dblabs.spatial.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Band

/**
  * The expression for extracting metadata from a raster band.
  * @param tileExpr
  *   The expression for the raster. If the raster is stored on disk, the path
  *   to the raster is provided. If the raster is stored in memory, the bytes of
  *   the raster are provided.
  * @param band
  *   The band index.
  */
case class RST_BandMetaData(
    tileExpr: Expression,
    band: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, band, ExpressionConfigExpr())
    override def dataType: DataType = MapType(StringType, StringType)
    override def nullable: Boolean = true
    override def prettyName: String = "rst_bandmetadata"
    override def replacement: Expression = rstInvoke(RST_BandMetaData, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_BandMetaData extends WithExpressionInfo {

    def evalPath(row: InternalRow, bandIndex: Int, conf: UTF8String): MapData = eval(row, bandIndex, conf, StringType)
    def evalBinary(row: InternalRow, bandIndex: Int, conf: UTF8String): MapData = eval(row, bandIndex, conf, BinaryType)

    def eval(row: InternalRow, bandIndex: Int, conf: UTF8String, dt: DataType): MapData = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val ds = RasterSerializationUtil.rowToDS(row, dt)
        val band = ds.GetRasterBand(bandIndex)
        val mtd = execute(band)
        band.delete()
        RasterDriver.releaseDataset(ds)
        SerializationUtil.toMapData[String, String](mtd)
    }

    def execute(band: Band): Map[String, String] = BandAccessors.getMetadata(band)

    override def name: String = "rst_bandmetadata"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_BandMetaData(c(0), c(1))

}
