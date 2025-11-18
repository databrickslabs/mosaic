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

case class BNG_CellUnion(
    leftChip: Expression,
    rightChip: Expression
) extends InvokedExpression {

    private def childType = leftChip.dataType.asInstanceOf[StructType].fields(0).dataType
    override def children: Seq[Expression] = Seq(leftChip, rightChip)
    override def dataType: DataType = BNG.cellType(childType)
    override def nullable: Boolean = true
    override def prettyName: String = BNG_CellUnion.name
    override def replacement: Expression =
        childType match {
            case LongType   => invoke(BNG_CellUnion, "evalLong")
            case StringType => invoke(BNG_CellUnion, "evalString")
        }
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

object BNG_CellUnion extends WithExpressionInfo {

    def evalLong(chip1: InternalRow, chip2: InternalRow): InternalRow = {
        // Note: we do check twice for early exit cases
        // that is a bit redundant but allows UDF callable abstraction
        // and avoids unnecessary WKB parsing at the same time
        if (chip1.getBoolean(1)) return chip1
        if (chip2.getBoolean(1)) return chip2
        val cellId = chip1.getLong(0)
        require(chip2.getLong(0) == cellId, "Can only union chips with the same grid cell id")
        val geom1 = JTS.fromWKB(chip1.getBinary(2))
        val geom2 = JTS.fromWKB(chip2.getBinary(2))
        val union = executeLong((cellId, chip1.getBoolean(1), geom1), (cellId, chip2.getBoolean(1), geom2))
        InternalRow.fromSeq(Seq(union._1, union._2, JTS.toWKB(union._3)))
    }

    def evalString(chip1: InternalRow, chip2: InternalRow): InternalRow = {
        // Note: we do check twice for early exit cases
        // that is a bit redundant but allows UDF callable abstraction
        // and avoids unnecessary WKB parsing at the same time
        if (chip1.getBoolean(1)) return chip1
        if (chip2.getBoolean(1)) return chip2
        val cellId = chip1.getString(0)
        require(chip2.getString(0) == cellId, "Can only union chips with the same grid cell id")
        val geom1 = JTS.fromWKB(chip1.getBinary(2))
        val geom2 = JTS.fromWKB(chip2.getBinary(2))
        val union = executeString((cellId, chip1.getBoolean(1), geom1), (cellId, chip2.getBoolean(1), geom2))
        InternalRow.fromSeq(Seq(UTF8String.fromString(union._1), union._2, JTS.toWKB(union._3)))
    }

    @inline def executeLong(chip1: (Long, Boolean, Geometry), chip2: (Long, Boolean, Geometry)): (Long, Boolean, Geometry) = {
        if (chip1._1 != chip2._1) (chip1._1, chip1._2, JTS.emptyPolygon)
        else {
            if (chip1._2) chip1
            else if (chip2._2) chip2
            else (chip1._1, chip1._2, chip1._3.union(chip2._3))
        }
    }

    @inline def executeString(chip1: (String, Boolean, Geometry), chip2: (String, Boolean, Geometry)): (String, Boolean, Geometry) = {
        if (chip1._1 != chip2._1) (chip1._1, chip1._2, JTS.emptyPolygon)
        else {
            if (chip1._2) chip1
            else if (chip2._2) chip2
            else (chip1._1, chip1._2, chip1._3.union(chip2._3))
        }
    }

    override def name: String = "gbx_bng_cellunion"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_CellUnion(c(0), c(1))

    //TODO: ADD EXPRESSION INFO
    override def usageArgs: String = ""

    override def description: String = ""

    override def extendedUsageArgs: String = ""

    override def examples: String = ""

}
