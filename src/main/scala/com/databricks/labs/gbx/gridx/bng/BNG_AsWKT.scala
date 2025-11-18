package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class BNG_AsWKT(
    cellID: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(cellID)
    override def dataType: DataType = StringType
    override def nullable: Boolean = true
    override def prettyName: String = BNG_AsWKT.name
    override def replacement: Expression = invoke(BNG_AsWKT)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

object BNG_AsWKT extends WithExpressionInfo {

    def eval(cellID: Long): UTF8String = UTF8String.fromString(execute(cellID))
    def eval(cellID: UTF8String): UTF8String = UTF8String.fromString(execute(cellID.toString))

    def execute(cellID: String): String = {
        val geom = BNG.cellIdToGeometry(BNG.parse(cellID))
        JTS.toWKT(geom)
    }

    def execute(cellID: Long): String = {
        val geom = BNG.cellIdToGeometry(cellID)
        JTS.toWKT(geom)
    }

    override def name: String = "gbx_bng_aswkt"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_AsWKT(c(0))

    //TODO: ADD EXPRESSION INFO
    override def usageArgs: String = ""

    override def description: String = ""

    override def extendedUsageArgs: String = ""

    override def examples: String = ""

}
