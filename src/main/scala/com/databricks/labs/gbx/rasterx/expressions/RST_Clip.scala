package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.{ClipToGeom, SpatialRefOps}
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset
import org.gdal.osr.SpatialReference
import org.locationtech.jts.geom.Geometry

import scala.util.{Failure, Try}

/** The expression for clipping a raster by a vector. */
case class RST_Clip(
    tileExpr: Expression,
    geometryExpr: Expression,
    cutlineAllTouchedExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, geometryExpr, cutlineAllTouchedExpr, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Clip.name
    override def replacement: Expression = rstInvoke(RST_Clip, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Clip extends WithExpressionInfo {

    def evalBinary(row: InternalRow, geom: Any, cutlineAllTouched: Boolean, conf: UTF8String): InternalRow =
        eval(row, geom, cutlineAllTouched, conf, BinaryType)
    def evalPath(row: InternalRow, geom: Any, cutlineAllTouched: Boolean, conf: UTF8String): InternalRow =
        eval(row, geom, cutlineAllTouched, conf, StringType)

    def eval(row: InternalRow, geom: Any, cutlineAllTouched: Boolean, conf: UTF8String, dt: DataType): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (_, ds, options) = RasterSerializationUtil.rowToTile(row, dt)
              val geometry = geom match {
                  case g: UTF8String  => JTS.fromWKT(g.toString)
                  case g: Array[Byte] => JTS.fromWKB(g)
              }
              val (resultDs, metadata) = execute(ds, options, geometry, cutlineAllTouched)
              RasterDriver.releaseDataset(ds)
              val res = RasterSerializationUtil.tileToRow((row.getLong(0), resultDs, metadata), dt, exprConf.hConf)
              RasterDriver.releaseDataset(resultDs)
              res
          },
          row,
          dt
        )

    def execute(ds: Dataset, options: Map[String, String], geom: Geometry, cutlineAllTouched: Boolean): (Dataset, Map[String, String]) = {
        val geomSR = new SpatialReference()
        val epsgCode = geom.getSRID
        Try(geomSR.ImportFromEPSG(epsgCode)) match {
            case Failure(_) =>
                val dsSR = ds.GetSpatialRef
                val dsEpsgCode = SpatialRefOps.getEPSGCode(dsSR)
                if (dsEpsgCode != 0) geomSR.ImportFromEPSG(dsEpsgCode)
                else geomSR.ImportFromWkt(dsSR.ExportToWkt())
            case _          => // Do nothing, we have a valid SRID
        }
        val res = ClipToGeom.clip(ds, options, geom, geomSR, cutlineAllTouched)
        geomSR.delete()
        res
    }

    override def name: String = "gbx_rst_clip"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Clip(c(0), c(1), c(2))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String = "Clips tile using provided clip Geometry in a supported encoding (WKB, WKT)."

    override def usageArgs: String = "tile, clip, cutline_all_touched"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(tile, clip_geom, true);
           |      ${_TILE_RESULT_}
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}, clip: <WKB | WKT>, cutline_all_touched: Boolean"

}
