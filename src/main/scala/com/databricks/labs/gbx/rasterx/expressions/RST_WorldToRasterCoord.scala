package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Returns the world coordinate of the raster. */
case class RST_WorldToRasterCoord(
    tileExpr: Expression,
    x: Expression,
    y: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, x, y, ExpressionConfigExpr())
    override def dataType: DataType = StructType(Seq(StructField("x", IntegerType), StructField("y", IntegerType)))
    override def nullable: Boolean = true
    override def prettyName: String = RST_WorldToRasterCoord.name
    override def replacement: Expression = rstInvoke(RST_WorldToRasterCoord, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_WorldToRasterCoord extends WithExpressionInfo {

    def evalPath(row: InternalRow, xGeo: Double, yGeo: Double, conf: UTF8String): InternalRow =
        eval(row, xGeo: Double, yGeo: Double, conf, StringType)
    def evalBinary(row: InternalRow, xGeo: Double, yGeo: Double, conf: UTF8String): InternalRow =
        eval(row, xGeo: Double, yGeo: Double, conf, BinaryType)

    def eval(row: InternalRow, xGeo: Double, yGeo: Double, conf: UTF8String, dt: DataType): InternalRow =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(row, dt)
                val res = execute(ds, xGeo, yGeo)
                RasterDriver.releaseDataset(ds)
                InternalRow.fromSeq(Seq(res._1, res._2))
            },
            row,
            dt,
            conf
          )
        ).map(_.asInstanceOf[InternalRow]).orNull

    def execute(ds: Dataset, xGeo: Double, yGeo: Double): (Int, Int) = GDAL.fromWorldCoord(ds.GetGeoTransform(), xGeo, yGeo)

    override def name: String = "gbx_rst_worldtorastercoord"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_WorldToRasterCoord(c(0), c(1), c(2))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        "Computes the (j, i) pixel coordinates of world_x and world_y within tile using the CRS of tile."

    override def usageArgs: String = "tile, world_x, world_y"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(tile, -160.1, 40.0) AS tile FROM table;
           |      {"x": 398, "y": 997}
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}, world_x: Double, world_y: Double"
}
