package com.dblabs.spatial.rasterx.expressions.constructor

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.rasterx.gdal.GDAL
import com.dblabs.spatial.rasterx.util.RST_ExpressionUtil
import com.dblabs.spatial.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
  * The raster for construction of a raster tile. This should be the first
  * expression in the expression tree for a raster tile.
  */
case class RST_FromContent(
    contentExpr: Expression,
    driverExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(contentExpr, driverExpr, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(BinaryType)
    override def nullable: Boolean = true
    override def prettyName: String = "rst_fromcontent"
    override def replacement: Expression = invoke(RST_FromContent)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Expression info required for the expression registration for spark SQL. */
object RST_FromContent extends WithExpressionInfo {

    def eval(content: Array[Byte], driver: UTF8String, conf: UTF8String): InternalRow = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val mtd = Map(
          "driver" -> driver.toString,
          "extension" -> GDAL.getExtension(driver.toString),
          "size" -> content.length.toString
        )
        val mapData = SerializationUtil.toMapData[String, String](mtd)
        val row = InternalRow.fromSeq(Seq(null, content, mapData))
        row
    }

    override def name: String = "rst_fromcontent"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_FromContent(c(0), c(1))


}
