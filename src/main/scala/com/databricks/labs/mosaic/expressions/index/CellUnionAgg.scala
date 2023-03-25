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

import scala.collection.mutable

case class CellUnionAgg(
    inputChip: Expression,
    geometryAPIName: String,
    indexSystem: IndexSystem,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[mutable.ArrayBuffer[Any]]
      with UnaryLike[Expression] {

    val geometryAPI: GeometryAPI = GeometryAPI.apply(geometryAPIName)
    override lazy val deterministic: Boolean = true
    override val child: Expression = inputChip
    override val nullable: Boolean = false
    override val dataType: DataType = ChipType(LongType)

    override def createAggregationBuffer(): mutable.ArrayBuffer[Any] = mutable.ArrayBuffer.empty

    def update(buffer: mutable.ArrayBuffer[Any], input: InternalRow): mutable.ArrayBuffer[Any] = {
        val value = child.eval(input)
        buffer += InternalRow.copyValue(value)
        buffer
    }

    def merge(buffer: mutable.ArrayBuffer[Any], input: mutable.ArrayBuffer[Any]): mutable.ArrayBuffer[Any] = {
        buffer ++= input
    }

    override def eval(buffer: mutable.ArrayBuffer[Any]): Any = {
        require(buffer.nonEmpty)

        val index_id = buffer.head.asInstanceOf[InternalRow].getLong(1)
        require(buffer.forall(p => p.asInstanceOf[InternalRow].getLong(1) == index_id), "All chips must have the same index_id")

        val core_chips = buffer.iterator.map(_.asInstanceOf[InternalRow]).filter(_.getBoolean(0)).toArray
        if (core_chips.length == 0) {
            buffer.head
            // buffer has only boundary cells and union operation is associative
            val union =
                buffer.iterator.map(_.asInstanceOf[InternalRow]).map(r => geometryAPI.geometry(r.getBinary(2), "WKB")).reduce(_.union(_))
            // the intersection _could_ create a new core chip. Leave this check out for performance reasons.
            InternalRow(false, index_id, union.toWKB)
        } else {
            // core_chips[0] == any element in core_chips
            core_chips.head
        }
    }

    private lazy val projection = UnsafeProjection.create(Array[DataType](ArrayType(elementType = dataType, containsNull = false)))
    private lazy val row = new UnsafeRow(1)

    override def serialize(obj: mutable.ArrayBuffer[Any]): Array[Byte] = {
        val array = new GenericArrayData(obj.toArray)
        projection.apply(InternalRow.apply(array)).getBytes
    }

    override def deserialize(bytes: Array[Byte]): mutable.ArrayBuffer[Any] = {
        val buffer = createAggregationBuffer()
        row.pointTo(bytes, bytes.length)
        row.getArray(0).foreach(dataType, (_, x: Any) => buffer += x)
        buffer
    }

    override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
        copy(mutableAggBufferOffset = newMutableAggBufferOffset)

    override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
        copy(inputAggBufferOffset = newInputAggBufferOffset)

    override def prettyName: String = "grid_cell_union_agg"

    override protected def withNewChildInternal(newChild: Expression): CellUnionAgg = copy(inputChip = newChild)

}

object CellUnionAgg {
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[CellUnionAgg].getCanonicalName,
          db.orNull,
          "grid_cell_union_agg",
          """
            |    _FUNC_(chip)) - Returns the union of a collection of chips based on a specific grid cell.
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
