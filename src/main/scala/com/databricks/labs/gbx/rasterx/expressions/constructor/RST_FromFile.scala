package com.databricks.labs.gbx.rasterx.expressions.constructor

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.GDAL
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil}
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
  * The raster for construction of a raster tile. This should be the first
  * expression in the expression tree for a raster tile.
  */
case class RST_FromFile(
    rasterPathExpr: Expression,
    driverExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(rasterPathExpr, driverExpr, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(StringType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_FromContent.name
    override def replacement: Expression = invoke(RST_FromFile)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_FromFile extends WithExpressionInfo {

    def eval(path: UTF8String, driver: UTF8String, conf: UTF8String): InternalRow =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val mtd = Map(
                  "driver" -> driver.toString,
                  "extension" -> GDAL.getExtension(driver.toString),
                  "size" -> -1.toString // size is unknown at this point
                )
                val mapData = SerializationUtil.toMapData[String, String](mtd)
                val row = InternalRow.fromSeq(Seq(null, path, mapData))
                row
            },
            null,
            StringType,
            conf
          )
        ).map(_.asInstanceOf[InternalRow]).orNull

    override def name: String = "gbx_rst_fromfile"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_FromFile(c(0), c(1))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        "Returns a raster tile from a file path."

    override def usageArgs: String = "path, driver"

    override def examples: String = {
        s"""
           |    Examples:
           |      > CREATE TABLE IF NOT EXISTS TABLE tbl
           |        USING binaryFile
           |        OPTIONS (path "/Volumes/...");
           |      > SELECT _FUNC_(path, 'GTiff') FROM tbl AS tile;
           |      ${_TILE_RESULT_}
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"path: String, driver: String"

}
