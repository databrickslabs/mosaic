package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.expressions._
import com.dblabs.spatial.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.Geometry

case class BNG_CellIntersection(
    leftChip: Expression,
    rightChip: Expression
) extends InvokedExpression
      with WithNewChildren {

    override def children: Seq[Expression] = Seq(leftChip, rightChip)
    override def dataType: DataType = leftChip.dataType
    override def nullable: Boolean = true
    override def prettyName: String = "bng_cell_intersection"
    override def replacement: Expression = {
        leftChip.dataType.asInstanceOf[StructType].fields(1).dataType match {
            case LongType => invoke(BNG_CellIntersection.LongID)
            case StringType => invoke(BNG_CellIntersection.StringID)
        }
    }

}

object BNG_CellIntersection extends WithExpressionInfo {

    object LongID {

        def eval(chip1: InternalRow, chip2: InternalRow): Any = {
            // Note: we do check twice for early exit cases
            // that is a bit redundant but allows UDF callable abstraction
            // and avoids unnecessary WKB parsing at the same time
            if (chip1.getBoolean(0)) return chip1
            if (chip2.getBoolean(0)) return chip2
            val cell1 = chip1.getLong(1)
            val cell2 = chip2.getLong(2)
            val geom1 = JTS.fromWKB(chip1.getBinary(2))
            val geom2 = JTS.fromWKB(chip2.getBinary(2))
            val res = eval((chip1.getBoolean(0), cell1, geom1), (chip1.getBoolean(0), cell2, geom2))
            InternalRow(res._1, res._2, JTS.toWKB(res._3))
        }

        def eval(chip1: (Boolean, Long, Geometry), chip2: (Boolean, Long, Geometry)): (Boolean, Long, Geometry) = {
            // Left hand rule, only chip1 survives intersection
            // if chips are different then empty intersection
            if (chip1._2 != chip2._2) (chip1._1, chip1._2, JTS.emptyPolygon)
            else {
                if (chip1._1) chip1
                else if (chip2._1) chip2
                else (chip1._1, chip1._2, chip1._3.intersection(chip2._3))
            }
        }

    }

    object StringID {

        def eval(chip1: InternalRow, chip2: InternalRow): Any = {
            // Note: we do check twice for early exit cases
            // that is a bit redundant but allows UDF callable abstraction
            // and avoids unnecessary WKB parsing at the same time
            if (chip1.getBoolean(0)) return chip1
            if (chip2.getBoolean(0)) return chip2
            val cell1 = chip1.getString(1)
            val cell2 = chip2.getString(2)
            val geom1 = JTS.fromWKB(chip1.getBinary(2))
            val geom2 = JTS.fromWKB(chip2.getBinary(2))
            val res = eval((chip1.getBoolean(0), cell1, geom1), (chip1.getBoolean(0), cell2, geom2))
            InternalRow(res._1, res._2, JTS.toWKB(res._3))
        }

        def eval(chip1: (Boolean, String, Geometry), chip2: (Boolean, String, Geometry)): (Boolean, String, Geometry) = {
            // Left hand rule, only chip1 survives intersection
            // if chips are different then empty intersection
            if (chip1._2 != chip2._2) (chip1._1, chip1._2, JTS.emptyPolygon)
            else {
                if (chip1._1) chip1
                else if (chip2._1) chip2
                else (chip1._1, chip1._2, chip1._3.intersection(chip2._3))
            }
        }

    }

    override def name: String = "bng_cellintersection"

    /**
      * Returns the expression builder (parser for spark SQL).
      *
      * @return
      *   An expression builder.
      */
    override def builder(expressionConfig: ExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[BNG_CellIntersection](2, expressionConfig)
    }

}
