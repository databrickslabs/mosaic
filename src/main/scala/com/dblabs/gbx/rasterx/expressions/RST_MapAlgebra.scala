package com.dblabs.gbx.rasterx.expressions

import com.dblabs.gbx.expressions._
import com.dblabs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.dblabs.gbx.rasterx.operations.MapAlgebra
import com.dblabs.gbx.rasterx.operator.{GDALCalc, GDALTranslate}
import com.dblabs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import com.dblabs.gbx.util.NodeFilePathUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, gdal}

import java.nio.file.{Files, Paths}
import scala.util.Try

/** The expression for map algebra. */
case class RST_MapAlgebra(
    tileExpr: Expression,
    jsonSpecExpr: Expression
) extends InvokedExpression {

    private def rasterType = tileExpr.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fields(1).dataType
    override def children: Seq[Expression] = Seq(tileExpr, jsonSpecExpr, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(rasterType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_MapAlgebra.name
    override def replacement: Expression = rstInvoke(RST_MapAlgebra, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_MapAlgebra extends WithExpressionInfo {

    def evalPath(array: ArrayData, spec: UTF8String, conf: UTF8String): InternalRow = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val dss = RasterSerializationUtil.arrayToTiles(array, StringType)
        val (result, mtd) = execute(dss.map(_._2), dss.head._3, spec.toString)
        dss.foreach(ds => RasterDriver.releaseDataset(ds._2))
        val res = RasterSerializationUtil.tileToRow((dss.head._1, result, mtd), StringType, exprConf.hConf)
        RasterDriver.releaseDataset(result)
        res
    }

    def evalBinary(array: ArrayData, spec: UTF8String, conf: UTF8String): InternalRow = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val dss = RasterSerializationUtil.arrayToTiles(array, BinaryType)
        // GDAL calc does not work with /vsimem/ files, so we need to copy them to a local path
        val dssCpy = dss.map { ds =>
            val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
            val extension = GDAL.getExtension(ds._2.GetDriver.getShortName)
            val path = s"${NodeFilePathUtil.rootPath}/$uuid.$extension"
            val (dsCpy, mtd) = GDALTranslate.executeTranslate(path, ds._2, "gdal_translate", ds._3)
            RasterDriver.releaseDataset(ds._2)
            (ds._1, dsCpy, mtd, path)
        }
        val (result, mtd) = execute(dssCpy.map(_._2), dss.head._3, spec.toString)
        val res = RasterSerializationUtil.tileToRow((dssCpy.head._1, result, mtd), BinaryType, exprConf.hConf)
        dssCpy.foreach(ds => RasterDriver.releaseDataset(ds._2))
        dssCpy.foreach(ds => Files.deleteIfExists(Paths.get(ds._4)))
        // result is computed via gdalcalc so it is not in /vsimem/, we need to delete it manually
        val resPath = result.GetDescription()
        RasterDriver.releaseDataset(result)
        Try(Files.deleteIfExists(Paths.get(resPath)))
        res
    }

    def execute(dss: Seq[Dataset], options: Map[String, String], spec: String): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val extension = GDAL.getExtension(dss.head.GetDriver.getShortName)
        val resultPath = s"${NodeFilePathUtil.rootPath}/map_algebra_$uuid.$extension" // s"/vsimem/map_algebra_$uuid.$extension"
        val command = MapAlgebra.parseSpec(spec, resultPath, dss)
        GDALCalc.executeCalc(command, resultPath, options, dss.head)
    }

    override def name: String = "gbx_rst_mapalgebra"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_MapAlgebra(c(0), c(1))

}
