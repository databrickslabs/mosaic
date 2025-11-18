package com.databricks.labs.gbx.vectorx.jts.legacy.expressions

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.vectorx.jts.JTS
import com.databricks.labs.gbx.vectorx.jts.legacy.InternalGeometry
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
    override def prettyName: String = ST_LegacyAsWKB.name
    override def replacement: Expression = invoke(ST_LegacyAsWKB)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

object ST_LegacyAsWKB extends WithExpressionInfo {

    def eval(legacyGeom: InternalRow): Array[Byte] = {
        val geom = InternalGeometry(legacyGeom).toJTS
        JTS.toWKB(geom)
    }

    override def name: String = "gbx_st_legacyaswkb"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new ST_LegacyAsWKB(c(0))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String = "Returns the legacy vector data format used by DBLabs Mosaic as WKB."

    override def usageArgs: String = "legacy_geom"

    override def examples: String = {
        s"""
          |    Examples:
          |      > SELECT _FUNC_(_ARGS_);
          |        [01 03 00 00 00 0...]
          |  """.stripMargin
    }

    // extended usage args might be useful
    override def extendedUsageArgs: String = "legacy_geom: <see vectorx.jts.legacy.InternalGeometry>"
}
