package com.databricks.labs.gbx.rasterx.expressions.accessors

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Returns the georeference of the raster. */
case class RST_GeoReference(
    tileExpr: Expression
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

    def eval(row: InternalRow, conf: UTF8String, rdt: DataType): MapData =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(row, rdt)
                val res = execute(ds)
                RasterDriver.releaseDataset(ds)
                SerializationUtil.toMapData[String, Double](res)
            },
            row,
            rdt,
            conf
          )
        ).map(_.asInstanceOf[MapData]).orNull

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

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        "Returns GeoTransform of the raster tile as a Map."

    override def usageArgs: String = "tile"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(_ARGS_) FROM table;
           |      {"scaleY": -0.049999999152053956, "skewX": 0, "skewY": 0, "upperLeftY": 89.99999847369712,
           |       "upperLeftX": -180.00000610436345, "scaleX": 0.050000001695656514}
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}"

    override def extendedDescription: String =
    """The output takes the form of a MapType with the following keys:
    | "upperLeftX" 	-> geoTransform(0)
    | "upperLeftY" 	-> geoTransform(3)
    | "scaleX" 	-> geoTransform(1)
    | "scaleY" 	-> geoTransform(5)
    | "skewX" 	-> geoTransform(2)
    | "skewY" 	-> geoTransform(4)""".stripMargin

}
