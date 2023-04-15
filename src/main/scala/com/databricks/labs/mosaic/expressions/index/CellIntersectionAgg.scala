package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.types.ChipType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

case class CellIntersectionAgg(
    inputChip: Expression,
    geometryAPIName: String,
    indexSystem: IndexSystem,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[ArrayBuffer[Any]]
      with UnaryLike[Expression] {

    val geometryAPI: GeometryAPI = GeometryAPI.apply(geometryAPIName)
    override lazy val deterministic: Boolean = true
    override val child: Expression = inputChip
    override val nullable: Boolean = false
    override val dataType: DataType = ChipType(LongType)

    override def createAggregationBuffer(): ArrayBuffer[Any] = ArrayBuffer.empty

    def update(buffer: ArrayBuffer[Any], input: InternalRow): ArrayBuffer[Any] = {
        val value = child.eval(input)
        buffer += InternalRow.copyValue(value)
        buffer
    }

    def merge(buffer: ArrayBuffer[Any], input: ArrayBuffer[Any]): ArrayBuffer[Any] = {
        buffer ++= input
    }

    override def eval(buffer: ArrayBuffer[Any]): Any = {
        require(buffer.nonEmpty)

        val index_id = buffer.head.asInstanceOf[InternalRow].getLong(1)
        require(
          buffer.forall(p => p.asInstanceOf[InternalRow].getLong(1) == index_id),
          "can only intersect chips based on the same grid cell"
        )

        val boundary_cells = buffer.iterator.map(_.asInstanceOf[InternalRow]).filter(r => !r.getBoolean(0)).toArray
        if (boundary_cells.length == 0) {
            // There are only core chips in the buffer. Just return the first chip of the buffer.
            buffer.head
        } else {
            // There is at least one boundary chip in the buffer. Core chips have no influence on the intersection.
            val intersection = boundary_cells.map(r => geometryAPI.geometry(r.getBinary(2), "WKB")).reduce(_.intersection(_))
            InternalRow(false, index_id, intersection.toWKB)
        }
    }

    private lazy val projection = UnsafeProjection.create(Array[DataType](ArrayType(elementType = dataType, containsNull = false)))
    private lazy val row = new UnsafeRow(1)

    override def serialize(obj: ArrayBuffer[Any]): Array[Byte] = {
        val array = new GenericArrayData(obj.toArray)
        projection.apply(InternalRow.apply(array)).getBytes
    }

    override def deserialize(bytes: Array[Byte]): ArrayBuffer[Any] = {
        val buffer = createAggregationBuffer()
        row.pointTo(bytes, bytes.length)
        row.getArray(0).foreach(dataType, (_, x: Any) => buffer += x)
        buffer
    }

    override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
        copy(mutableAggBufferOffset = newMutableAggBufferOffset)

    override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
        copy(inputAggBufferOffset = newInputAggBufferOffset)

    override def prettyName: String = "grid_cell_intersection_agg"

    override protected def withNewChildInternal(newChild: Expression): CellIntersectionAgg = copy(inputChip = newChild)

}

object CellIntersectionAgg {
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[CellIntersectionAgg].getCanonicalName,
          db.orNull,
          "grid_cell_intersection_agg",
          """
            |    _FUNC_(chip)) - Returns the intersection of a collection of chips based on a specific grid cell.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a);
            |        {is_core: false, index_id: 590418571381702655, wkb: ...}
            |  """.stripMargin,
          "",
          "agg_funcs",
          "1.0",
          "",
          "built-in"
        )

}
