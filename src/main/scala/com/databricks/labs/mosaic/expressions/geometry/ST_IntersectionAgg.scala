package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.expressions.index.IndexGeometry
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types._

case class ST_IntersectionAgg(
    leftChip: Expression,
    rightChip: Expression,
    geometryAPIName: String,
    indexSystem: IndexSystem,
    mutableAggBufferOffset: Int,
    inputAggBufferOffset: Int
) extends TypedImperativeAggregate[Array[Byte]]
      with BinaryLike[Expression] {

    val geometryAPI: GeometryAPI = GeometryAPI.apply(geometryAPIName)
    override lazy val deterministic: Boolean = true
    override val left: Expression = leftChip
    override val right: Expression = rightChip
    override val nullable: Boolean = false
    override val dataType: DataType = BinaryType
    private val emptyWKB = geometryAPI.geometry("POLYGON(EMPTY)", "WKT").toWKB

    override def prettyName: String = "st_intersection_agg"

    private[geometry] def getCellGeom(row: InternalRow, dt: DataType) = {
        dt.asInstanceOf[StructType].fields.find(_.name == "index_id").map(_.dataType) match {
            case Some(LongType)   => indexSystem.indexToGeometry(row.getLong(1), geometryAPI)
            case Some(StringType) => indexSystem.indexToGeometry(indexSystem.parse(row.getString(1)), geometryAPI)
            case _                => throw new Error("Unsupported format for chips.")
        }
    }

    override def update(accumulator: Array[Byte], inputRow: InternalRow): Array[Byte] = {
        val state = accumulator
        val partialGeom = geometryAPI.geometry(state, "WKB")

        val leftIndexValue = left.eval(inputRow).asInstanceOf[InternalRow]
        val rightIndexValue = right.eval(inputRow).asInstanceOf[InternalRow]

        val leftCoreFlag = leftIndexValue.getBoolean(0)
        val rightCoreFlag = rightIndexValue.getBoolean(0)

        val geomIncrement =
            if (leftCoreFlag && rightCoreFlag) {
                getCellGeom(leftIndexValue, leftChip.dataType)
            } else if (leftCoreFlag) {
                geometryAPI.geometry(rightIndexValue.getBinary(2), "WKB")
            } else if (rightCoreFlag) {
                geometryAPI.geometry(leftIndexValue.getBinary(2), "WKB")
            } else {
                val leftChipGeom = geometryAPI.geometry(leftIndexValue.getBinary(2), "WKB")
                val rightChipGeom = geometryAPI.geometry(rightIndexValue.getBinary(2), "WKB")
                leftChipGeom.intersection(rightChipGeom)
            }

        partialGeom.union(geomIncrement).toWKB
    }

    override def merge(accumulator: Array[Byte], input: Array[Byte]): Array[Byte] = {
        val leftPartial = accumulator
        val rightPartial = input
        val leftPartialGeom = geometryAPI.geometry(leftPartial, "WKB")
        val rightPartialGeom = geometryAPI.geometry(rightPartial, "WKB")
        val result = leftPartialGeom.union(rightPartialGeom)
        result.toWKB
    }

    override def createAggregationBuffer(): Array[Byte] = emptyWKB

    override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
        copy(inputAggBufferOffset = newInputAggBufferOffset)

    override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
        copy(mutableAggBufferOffset = newMutableAggBufferOffset)

    override def eval(accumulator: Array[Byte]): Any = accumulator

    override def serialize(accumulator: Array[Byte]): Array[Byte] = accumulator

    override def deserialize(storageFormat: Array[Byte]): Array[Byte] = storageFormat

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): ST_IntersectionAgg =
        copy(leftChip = newLeft, rightChip = newRight)

}

object ST_IntersectionAgg {

    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[IndexGeometry].getCanonicalName,
          db.orNull,
          "st_intersection_agg",
          """
            |    _FUNC_(left_index, right_index)) - Resolves an intersection geometry based on matched indices.
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
