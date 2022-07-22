package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.expressions.index.IndexGeometry
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types._

case class ST_IntersectsAggregate(
    leftChip: Expression,
    rightChip: Expression,
    geometryAPIName: String,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[Boolean]
      with BinaryLike[Expression] {

    val geometryAPI: GeometryAPI = GeometryAPI.apply(geometryAPIName)
    override val left: Expression = leftChip
    override val right: Expression = rightChip
    override val nullable: Boolean = false
    override val dataType: DataType = BooleanType

    override def prettyName: String = "st_intersects_aggregate"

    override def update(accumulator: Boolean, inputRow: InternalRow): Boolean = {
        accumulator || {
            val leftChipValue = left.eval(inputRow).asInstanceOf[InternalRow]
            val rightChipValue = right.eval(inputRow).asInstanceOf[InternalRow]
            leftChipValue.getBoolean(0) || rightChipValue.getBoolean(0) || {
                val leftChipGeom = geometryAPI.geometry(leftChipValue.getBinary(2), "WKB")
                val rightChipGeom = geometryAPI.geometry(rightChipValue.getBinary(2), "WKB")
                leftChipGeom.intersects(rightChipGeom)
            }
        }
    }

    override def merge(accumulator: Boolean, input: Boolean): Boolean = {
        accumulator || input
    }

    override def createAggregationBuffer(): Boolean = false

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

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): ST_IntersectsAggregate =
        copy(leftChip = newLeft, rightChip = newRight)

}

object ST_IntersectsAggregate {

    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[IndexGeometry].getCanonicalName,
          db.orNull,
          "st_intersects_aggregate",
          """
            |    _FUNC_(left_index, right_index)) - Resolves an intersects based on matched indices.
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
