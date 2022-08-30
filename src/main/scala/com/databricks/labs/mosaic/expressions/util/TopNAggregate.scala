package com.databricks.labs.mosaic.expressions.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Collect, ImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.util.{GenericArrayData, TypeUtils}
import org.apache.spark.sql.types._

import scala.collection.mutable

@ExpressionDescription(
  usage = "_FUNC_(expr) - Collects and returns a list of non-unique elements.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (1), (2), (1) AS tab(col);
       [1,2,1]
               """,
  note = """
    The function is non-deterministic because the order of collected results depends
    on the order of the rows which may be non-deterministic after a shuffle.
           """,
  group = "agg_funcs",
  since = "2.0.0"
)
case class TopNAggregate(
    child: Expression,
    n: Int,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends Collect[mutable.PriorityQueue[Any]]
      with UnaryLike[Expression] {

    override lazy val deterministic: Boolean = true
    override lazy val bufferElementType: DataType = child.dataType
    override val nullable: Boolean = false

    def this(child: Expression) = this(child, 0, 0)

    override def dataType: DataType = ArrayType(child.dataType, containsNull = false)

    override def prettyName: String = "top_n_agg"

    override def update(accumulator: mutable.PriorityQueue[Any], inputRow: InternalRow): mutable.PriorityQueue[Any] = {
        val state = accumulator
        val childValue = child.eval(inputRow).asInstanceOf[InternalRow]
        state.enqueue(childValue)
        takeN(state)
    }

    override def merge(accumulator: mutable.PriorityQueue[Any], input: mutable.PriorityQueue[Any]): mutable.PriorityQueue[Any] = {
        val leftPartial = accumulator
        val rightPartial = input
        rightPartial.foreach(leftPartial.enqueue(_))
        takeN(leftPartial)
    }

    override def eval(accumulator: mutable.PriorityQueue[Any]): Any = {
        new GenericArrayData(accumulator.dequeueAll)
    }

    override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
        copy(inputAggBufferOffset = newInputAggBufferOffset)

    override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
        copy(mutableAggBufferOffset = newMutableAggBufferOffset)

    override def withNewChildInternal(newChild: Expression): TopNAggregate = copy(child = newChild)

    override protected def convertToBufferElement(value: Any): Any = InternalRow.copyValue(value)

    private def takeN(accumulator: mutable.PriorityQueue[Any]): mutable.PriorityQueue[Any] = {
        if (accumulator.size > n) {
            val newAcc = createAggregationBuffer()
            while (newAcc.length < n) {
                newAcc.enqueue(accumulator.dequeue())
            }
            newAcc
        } else {
            accumulator
        }
    }

    override def createAggregationBuffer(): mutable.PriorityQueue[Any] =
        mutable.PriorityQueue.empty[Any](TypeUtils.getInterpretedOrdering(child.dataType))

}
