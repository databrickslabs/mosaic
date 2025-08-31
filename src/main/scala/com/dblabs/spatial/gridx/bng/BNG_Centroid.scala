package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.gridx.grid.BNG
import com.dblabs.spatial.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class BNG_Centroid(
    cellID: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(cellID)
    override def dataType: DataType = BinaryType
    override def nullable: Boolean = true
    override def prettyName: String = "bng_centroid"
    override def replacement: Expression = invoke(BNG_Centroid)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

object BNG_Centroid extends WithExpressionInfo {

    def eval(cellID: Long): Array[Byte] = execute(cellID)
    def eval(cellID: UTF8String): Array[Byte] = execute(cellID.toString)

    def execute(cellID: Long): Array[Byte] = {
        val geom = BNG.cellIdToGeometry(cellID).getCentroid
        JTS.toWKB(geom)
    }

    def execute(cellID: String): Array[Byte] = {
        val geom = BNG.cellIdToGeometry(BNG.parse(cellID)).getCentroid
        JTS.toWKB(geom)
    }

    override def name: String = "bng_centroid"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_Centroid(c(0))

}
