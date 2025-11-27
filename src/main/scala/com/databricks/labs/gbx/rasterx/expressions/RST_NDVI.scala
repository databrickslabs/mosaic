package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operations.NDVI
import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import com.databricks.labs.gbx.util.NodeFilePathUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

import java.nio.file.{Files, Paths}
import scala.util.Try

/** The expression for computing NDVI index. */
case class RST_NDVI(
    tileExpr: Expression,
    redIndex: Expression,
    nirIndex: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, redIndex, nirIndex, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = RST_NDVI.name
    override def replacement: Expression = rstInvoke(RST_NDVI, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_NDVI extends WithExpressionInfo {

    def evalPath(row: InternalRow, redIndex: Int, nirIndex: Int, conf: UTF8String): InternalRow = {
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, mtd) = RasterSerializationUtil.rowToTile(row, StringType)
              val (resultDs, resMtd) = execute(ds, redIndex, nirIndex, mtd)
              RasterDriver.releaseDataset(ds)
              RasterSerializationUtil.tileToRow((cell, resultDs, resMtd), StringType, exprConf.hConf)
          },
          row,
          StringType
        )

    }
    def evalBinary(row: InternalRow, redIndex: Int, nirIndex: Int, conf: UTF8String): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, mtd) = RasterSerializationUtil.rowToTile(row, BinaryType)
              val extension = GDAL.getExtension(ds.GetDriver.getShortName)
              val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
              val cpyPath = s"${NodeFilePathUtil.rootPath}/ndvi_temp_$uuid.$extension"
              val (dsCpy, dsMtd) = GDALTranslate.executeTranslate(cpyPath, ds, "gdal_translate", mtd)
              val (resultDs, resMtd) = execute(dsCpy, redIndex, nirIndex, dsMtd)
              if (resultDs == null) {
                  throw new Error(
                      s"""
                         |NDVI computation failed.
                         |${org.gdal.gdal.gdal.GetLastErrorMsg()}
                         |$resMtd
                         |""".stripMargin)
              }
              val resPath = resultDs.GetDescription()
              RasterDriver.releaseDataset(ds)
              RasterDriver.releaseDataset(dsCpy)
              Try(Files.deleteIfExists(Paths.get(cpyPath))) // ndvi_temp file is not stored in /vsimem/ so we need to delete it
              val res = RasterSerializationUtil.tileToRow((cell, resultDs, resMtd), BinaryType, exprConf.hConf)
              Try(Files.deleteIfExists(Paths.get(resPath))) // resultDs is not stored in /vsimem/ so we need to delete it
              RasterDriver.releaseDataset(resultDs)
              res
          },
          row,
          BinaryType
        )

    def execute(ds: Dataset, redIndex: Int, nirIndex: Int, options: Map[String, String]): (Dataset, Map[String, String]) = {
        NDVI.compute(ds, options, redIndex, nirIndex)
    }

    override def name: String = "gbx_rst_ndvi"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_NDVI(c(0), c(1), c(2))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        "Calculates the Normalized Difference Vegetation Index (NDVI) for a raster."

    override def usageArgs: String = "tile, red_band, nir_band"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(tile, 1, 2) AS tile FROM table;
           |      ${_TILE_RESULT_}
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}, red_band: Int, nir_band: Int"

}
