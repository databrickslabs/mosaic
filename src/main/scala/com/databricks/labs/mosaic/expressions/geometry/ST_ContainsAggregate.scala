package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemID}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types._

case class ST_ContainsAggregate(
    leftChip: Expression,
    rightChip: Expression,
    geometryAPIName: String,
    indexSystemName: String,
    mutableAggBufferOffset: Int,
    inputAggBufferOffset: Int
) extends TypedImperativeAggregate[Boolean]
      with BinaryLike[Expression] {

    lazy val geometryAPI: GeometryAPI = GeometryAPI.apply(geometryAPIName)
    lazy val indexSystem: IndexSystem = IndexSystemID.getIndexSystem(IndexSystemID.apply(indexSystemName))
    override lazy val deterministic: Boolean = true
    override val left: Expression = leftChip
    override val right: Expression = rightChip
    override val nullable: Boolean = false
    override val dataType: DataType = BooleanType

    override def prettyName: String = "st_contains_aggregate"

    override def update(accumulator: Boolean, inputRow: InternalRow): Boolean = {
        if (!accumulator) {
            // Contains relation requires all right chips to satisfy contains
            // conditions. If at any point accumulator is false we can
            // short-circuit the aggregation.
            false
        } else {
            val leftIndexValue = left.eval(inputRow).asInstanceOf[InternalRow]
            val rightIndexValue = right.eval(inputRow).asInstanceOf[InternalRow]
            val leftCoreFlag = leftIndexValue.getBoolean(0)
            val rightCoreFlag = rightIndexValue.getBoolean(0)

            if (rightCoreFlag) {
                // If the right chip is core, then left contains right
                // if and only if when left chip is core and accumulator is true
                accumulator && leftCoreFlag
            } else {
                // If any of the chips isnt core then contains relation has
                // to be evaluated. If accumulator is already false then no
                // need to evaluate anything.
                val leftChipGeom = geometryAPI.geometry(leftIndexValue.getBinary(2), "WKB")
                val rightChipGeom = geometryAPI.geometry(rightIndexValue.getBinary(2), "WKB")
                accumulator && leftChipGeom.contains(rightChipGeom)
            }
        }
    }

    override def merge(accumulator: Boolean, input: Boolean): Boolean = {
        accumulator && input
    }

    override def createAggregationBuffer(): Boolean = true

    override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
        copy(inputAggBufferOffset = newInputAggBufferOffset)

    override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
        copy(mutableAggBufferOffset = newMutableAggBufferOffset)

    override def eval(accumulator: Boolean): Any = accumulator

    override def serialize(buffer: Boolean): Array[Byte] = {
        if (buffer) Array(1.asInstanceOf[Byte]) else Array(0.asInstanceOf[Byte])
    }

    override def deserialize(storageFormat: Array[Byte]): Boolean = {
        storageFormat.head.equals(1.asInstanceOf[Byte])
    }

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): ST_ContainsAggregate =
        copy(leftChip = newLeft, rightChip = newRight)

}

object ST_ContainsAggregate {

    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_ContainsAggregate].getCanonicalName,
          db.orNull,
          "st_contains_aggregate",
          """
            |    _FUNC_(left_index, right_index)) - Resolves a contains based on matched indices.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, b);
            |        geom
            |        geom
            |        ...
            |        geom
            |  """.stripMargin,
          "",
          "agg_funcs",
          "1.0",
          "",
          "built-in"
        )

}
