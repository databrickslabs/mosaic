package com.dblabs.spatial.rasterx.expressions

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.rasterx.gdal.{GDAL, RasterDriver}
import com.dblabs.spatial.rasterx.operations.MapAlgebra
import com.dblabs.spatial.rasterx.operator.GDALCalc
import com.dblabs.spatial.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** The expression for map algebra. */
case class RST_MapAlgebra(
    tileExpr: Expression,
    jsonSpecExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, jsonSpecExpr, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = "rst_mapalgebra"
    override def replacement: Expression = rstInvoke(RST_MapAlgebra, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_MapAlgebra extends WithExpressionInfo {

    def evalPath(array: ArrayData, spec: UTF8String, conf: UTF8String): InternalRow = eval(array, spec, conf, StringType)
    def evalBinary(array: ArrayData, spec: UTF8String, conf: UTF8String): InternalRow = eval(array, spec, conf, BinaryType)

    def eval(array: ArrayData, spec: UTF8String, conf: UTF8String, rdt: DataType): InternalRow = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val dss = RasterSerializationUtil.arrayToTiles(array, rdt)
        val (result, mtd) = execute(dss.map(_._2), dss.head._3, spec.toString)
        dss.foreach(ds => RasterDriver.releaseDataset(ds._2))
        RasterSerializationUtil.tileToRow((dss.head._1, result, mtd), rdt, exprConf.hConf)
    }

    def execute(dss: Seq[Dataset], options: Map[String, String], spec: String): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val extension = GDAL.getExtension(dss.head.GetDriver.getShortName)
        val resultPath = s"/vsimem/map_algebra_$uuid.$extension"
        val command = MapAlgebra.parseSpec(spec, resultPath, dss)
        GDALCalc.executeCalc(command, resultPath, options, dss.head)
    }

    override def name: String = "rst_mapalgebra"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_MapAlgebra(c(0), c(1))


}
