package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.gridx.grid.BNG
import com.dblabs.spatial.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.Geometry

case class BNG_CellUnion(
    leftChip: Expression,
    rightChip: Expression
) extends InvokedExpression
      with WithNewChildren {

    private val childType = leftChip.dataType.asInstanceOf[StructType].fields(1).dataType
    override def children: Seq[Expression] = Seq(leftChip, rightChip)
    override def dataType: DataType = BNG.cellType(childType)
    override def nullable: Boolean = true
    override def prettyName: String = "bng_cellunion"
    override def replacement: Expression =
        childType match {
            case LongType   => invoke(BNG_CellUnion, "evalLong")
            case StringType => invoke(BNG_CellUnion, "evalString")
        }

}

object BNG_CellUnion extends WithExpressionInfo {

    def evalLong(chip1: InternalRow, chip2: InternalRow): InternalRow = {
        // Note: we do check twice for early exit cases
        // that is a bit redundant but allows UDF callable abstraction
        // and avoids unnecessary WKB parsing at the same time
        if (chip1.getBoolean(0)) return chip1
        if (chip2.getBoolean(0)) return chip2
        val cellId = chip1.getLong(1)
        require(chip2.getLong(1) == cellId, "Can only union chips with the same grid cell id")
        val geom1 = JTS.fromWKB(chip1.getBinary(2))
        val geom2 = JTS.fromWKB(chip2.getBinary(2))
        val union = evalLong((chip1.getBoolean(0), cellId, geom1), (chip2.getBoolean(0), cellId, geom2))
        InternalRow.fromSeq(Seq(union._1, union._2, JTS.toWKB(union._3)))
    }

    def evalLong(chip1: (Boolean, Long, Geometry), chip2: (Boolean, Long, Geometry)): (Boolean, Long, Geometry) = {
        if (chip1._2 != chip2._2) (chip1._1, chip1._2, JTS.emptyPolygon)
        else {
            if (chip1._1) chip1
            else if (chip2._1) chip2
            else (chip1._1, chip1._2, chip1._3.union(chip2._3))
        }
    }

    def evalString(chip1: InternalRow, chip2: InternalRow): InternalRow = {
        // Note: we do check twice for early exit cases
        // that is a bit redundant but allows UDF callable abstraction
        // and avoids unnecessary WKB parsing at the same time
        if (chip1.getBoolean(0)) return chip1
        if (chip2.getBoolean(0)) return chip2
        val cellId = chip1.getString(1)
        require(chip2.getString(1) == cellId, "Can only union chips with the same grid cell id")
        val geom1 = JTS.fromWKB(chip1.getBinary(2))
        val geom2 = JTS.fromWKB(chip2.getBinary(2))
        val union = evalString((chip1.getBoolean(0), cellId, geom1), (chip2.getBoolean(0), cellId, geom2))
        InternalRow.fromSeq(Seq(union._1, union._2, JTS.toWKB(union._3)))
    }

    def evalString(chip1: (Boolean, String, Geometry), chip2: (Boolean, String, Geometry)): (Boolean, String, Geometry) = {
        if (chip1._2 != chip2._2) (chip1._1, chip1._2, JTS.emptyPolygon)
        else {
            if (chip1._1) chip1
            else if (chip2._1) chip2
            else (chip1._1, chip1._2, chip1._3.union(chip2._3))
        }
    }

    override def name: String = "bng_cellunion"

    override def builder(expressionConfig: ExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[BNG_CellUnion](2, expressionConfig)
    }

}
