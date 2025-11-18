package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class BNG_AsWKB(
    cellID: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(cellID)
    override def dataType: DataType = BinaryType
    override def nullable: Boolean = true
    override def prettyName: String = BNG_AsWKB.name
    override def replacement: Expression = invoke(BNG_AsWKB)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

object BNG_AsWKB extends WithExpressionInfo {

    def eval(cellID: Long): Array[Byte] = execute(cellID)
    def eval(cellID: UTF8String): Array[Byte] = execute(cellID.toString)

    def execute(cellID: Long): Array[Byte] = {
        val geom = BNG.cellIdToGeometry(cellID)
        JTS.toWKB(geom)
    }

    def execute(cellID: String): Array[Byte] = {
        val geom = BNG.cellIdToGeometry(BNG.parse(cellID))
        JTS.toWKB(geom)
    }

    override def name: String = "gbx_bng_aswkb"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_AsWKB(c(0))

    //TODO: ADD EXPRESSION INFO
    override def usageArgs: String = ""

    override def description: String = ""

    override def extendedUsageArgs: String = ""

    override def examples: String = ""
}
