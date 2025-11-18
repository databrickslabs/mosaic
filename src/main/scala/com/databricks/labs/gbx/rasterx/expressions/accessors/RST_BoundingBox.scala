package com.databricks.labs.gbx.rasterx.expressions.accessors

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.BoundingBox
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset
import org.locationtech.jts.geom.Geometry

/** The expression for extracting the bounding box of a raster. */
case class RST_BoundingBox(
    tileExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def dataType: DataType = BinaryType
    override def nullable: Boolean = true
    override def prettyName: String = RST_BoundingBox.name
    override def replacement: Expression = rstInvoke(RST_BoundingBox, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_BoundingBox extends WithExpressionInfo {

    def evalPath(row: InternalRow, conf: UTF8String): Array[Byte] = eval(row, conf, StringType)
    def evalBinary(row: InternalRow, conf: UTF8String): Array[Byte] = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, dt: DataType): Array[Byte] =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(row, dt)
                val bbox = execute(ds)
                RasterDriver.releaseDataset(ds)
                JTS.toWKB(bbox)
            },
            row,
            dt,
            conf
          )
        ).map(_.asInstanceOf[Array[Byte]]).orNull

    def execute(ds: Dataset): Geometry = BoundingBox.bbox(ds, ds.GetSpatialRef)

    override def name: String = "gbx_rst_boundingbox"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_BoundingBox(c(0))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        "Returns the bounding box of the raster as a polygon geometry (WKB)."

    override def usageArgs: String = "tile"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(_ARGS_) FROM table;
           |      [00 03 ... 00]
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}"

}
