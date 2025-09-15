package com.dblabs.spatial.rasterx.expressions

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.rasterx.gdal.{GDAL, RasterDriver}
import com.dblabs.spatial.rasterx.operations.MapAlgebra
import com.dblabs.spatial.rasterx.operator.{GDALCalc, GDALTranslate}
import com.dblabs.spatial.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import com.dblabs.spatial.util.NodeFilePathUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** The expression for map algebra. */
case class RST_MapAlgebra(
    tileExpr: Expression,
    jsonSpecExpr: Expression
) extends InvokedExpression {

    private def rasterType = tileExpr.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fields(1).dataType
    override def children: Seq[Expression] = Seq(tileExpr, jsonSpecExpr, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(rasterType)
    override def nullable: Boolean = true
    override def prettyName: String = "rst_mapalgebra"
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
        RasterSerializationUtil.tileToRow((dss.head._1, result, mtd), StringType, exprConf.hConf)
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
            (ds._1, dsCpy, mtd)
        }
        val (result, mtd) = execute(dssCpy.map(_._2), dss.head._3, spec.toString)
        RasterSerializationUtil.tileToRow((dssCpy.head._1, result, mtd), BinaryType, exprConf.hConf)
    }

    def execute(dss: Seq[Dataset], options: Map[String, String], spec: String): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val extension = GDAL.getExtension(dss.head.GetDriver.getShortName)
        val resultPath = s"${NodeFilePathUtil.rootPath}/map_algebra_$uuid.$extension" // s"/vsimem/map_algebra_$uuid.$extension"
        val command = MapAlgebra.parseSpec(spec, resultPath, dss)
        GDALCalc.executeCalc(command, resultPath, options, dss.head)
    }

    override def name: String = "rst_mapalgebra"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_MapAlgebra(c(0), c(1))

}
