package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

case class BNG_CellIntersection(
    leftChip: Expression,
    rightChip: Expression
) extends InvokedExpression {

    private def childType = leftChip.dataType.asInstanceOf[StructType].fields(0).dataType
    override def children: Seq[Expression] = Seq(leftChip, rightChip)
    override def dataType: DataType = BNG.cellType(childType)
    override def nullable: Boolean = true
    override def prettyName: String = BNG_CellIntersection.name
    override def replacement: Expression =
        childType match {
            case LongType   => invoke(BNG_CellIntersection, "evalLong")
            case StringType => invoke(BNG_CellIntersection, "evalString")
        }
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

object BNG_CellIntersection extends WithExpressionInfo {

    def evalLong(chip1: InternalRow, chip2: InternalRow): InternalRow = {
        // Note: we do check twice for early exit cases
        // that is a bit redundant but allows UDF callable abstraction
        // and avoids unnecessary WKB parsing at the same time
        if (chip1.getBoolean(1)) return chip1
        if (chip2.getBoolean(1)) return chip2
        val cell1 = chip1.getLong(0)
        val cell2 = chip2.getLong(0)
        val geom1 = JTS.fromWKB(chip1.getBinary(2))
        val geom2 = JTS.fromWKB(chip2.getBinary(2))
        val res = executeLong((cell1, chip1.getBoolean(1), geom1), (cell2, chip2.getBoolean(1), geom2))
        InternalRow.fromSeq(Seq(res._1, res._2, JTS.toWKB(res._3)))
    }

    def evalString(chip1: InternalRow, chip2: InternalRow): InternalRow = {
        // Note: we do check twice for early exit cases
        // that is a bit redundant but allows UDF callable abstraction
        // and avoids unnecessary WKB parsing at the same time
        if (chip1.getBoolean(1)) return chip1
        if (chip2.getBoolean(1)) return chip2
        val cell1 = chip1.getString(0)
        val cell2 = chip2.getString(0)
        val geom1 = JTS.fromWKB(chip1.getBinary(2))
        val geom2 = JTS.fromWKB(chip2.getBinary(2))
        val res = executeString((cell1, chip1.getBoolean(1), geom1), (cell2, chip2.getBoolean(1), geom2))
        InternalRow(UTF8String.fromString(res._1), res._2, JTS.toWKB(res._3))
    }

    def executeLong(chip1: (Long, Boolean, Geometry), chip2: (Long, Boolean, Geometry)): (Long, Boolean, Geometry) = {
        // Left hand rule, only chip1 survives intersection
        // if chips are different then empty intersection
        if (chip1._1 != chip2._1) (chip1._1, chip1._2, JTS.emptyPolygon)
        else {
            if (chip1._2) chip1
            else if (chip2._2) chip2
            else (chip1._1, chip1._2, chip1._3.intersection(chip2._3))
        }
    }

    def executeString(chip1: (String, Boolean, Geometry), chip2: (String, Boolean, Geometry)): (String, Boolean, Geometry) = {
        // Left hand rule, only chip1 survives intersection
        // if chips are different then empty intersection
        if (chip1._1 != chip2._1) (chip1._1, chip1._2, JTS.emptyPolygon)
        else {
            if (chip1._2) chip1
            else if (chip2._2) chip2
            else (chip1._1, chip1._2, chip1._3.intersection(chip2._3))
        }
    }

    override def name: String = "gbx_bng_cellintersection"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_CellIntersection(c(0), c(1))

    //TODO: ADD EXPRESSION INFO
    override def usageArgs: String = ""

    override def description: String = ""

    override def extendedUsageArgs: String = ""

    override def examples: String = ""

}
