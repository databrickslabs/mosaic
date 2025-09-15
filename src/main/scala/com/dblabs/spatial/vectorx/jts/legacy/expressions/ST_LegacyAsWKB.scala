package com.dblabs.spatial.vectorx.jts.legacy.expressions

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.vectorx.jts.JTS
import com.dblabs.spatial.vectorx.jts.legacy.InternalGeometry
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

case class ST_LegacyAsWKB(
    geom: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(geom)
    override def dataType: DataType = BinaryType
    override def nullable: Boolean = true
    override def prettyName: String = "st_legacyaswkb"
    override def replacement: Expression = invoke(ST_LegacyAsWKB)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

object ST_LegacyAsWKB extends WithExpressionInfo {

    def eval(legacyGeom: InternalRow): Array[Byte] = {
        val geom = InternalGeometry(legacyGeom).toJTS
        JTS.toWKB(geom)
    }

    override def name: String = "st_legacyaswkb"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new ST_LegacyAsWKB(c(0))

}
