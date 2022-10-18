package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types._

case class ST_UnionAgg(
    inputGeom: Expression,
    geometryAPIName: String,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[Array[Byte]]
      with UnaryLike[Expression] {

    val geometryAPI: GeometryAPI = GeometryAPI.apply(geometryAPIName)
    override def child: Expression = inputGeom
    override val nullable: Boolean = false
    override val dataType: DataType = BinaryType
    private val emptyWKB = geometryAPI.geometry("POLYGON(EMPTY)", "WKT").toWKB

    override def prettyName: String = "st_union_agg"

    override def update(accumulator: Array[Byte], inputRow: InternalRow): Array[Byte] = {
        val accGeom = geometryAPI.geometry(accumulator, "WKB")
        val inGeom = geometryAPI.geometry(child.eval(inputRow), child.dataType)

        val newPartialGeom = accGeom.union(inGeom).toWKB
        newPartialGeom
    }

    override def merge(accumulator: Array[Byte], input: Array[Byte]): Array[Byte] = {
        val accGeom = geometryAPI.geometry(accumulator, "WKB")
        val inGeom = geometryAPI.geometry(input, "WKB")

        accGeom.union(inGeom).toWKB
    }

    override def createAggregationBuffer(): Array[Byte] = emptyWKB

    override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
        copy(inputAggBufferOffset = newInputAggBufferOffset)

    override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
        copy(mutableAggBufferOffset = newMutableAggBufferOffset)

    override def eval(accumulator: Array[Byte]): Any = accumulator

    override def serialize(accumulator: Array[Byte]): Array[Byte] = accumulator

    override def deserialize(storageFormat: Array[Byte]): Array[Byte] = storageFormat

    override protected def withNewChildInternal(newChild: Expression): ST_UnionAgg = copy(newChild)

}

object ST_UnionAgg {
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_UnionAgg].getCanonicalName,
          db.orNull,
          "st_union_agg",
          """
            |    _FUNC_(geom) - aggregates the union of all input geometries.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(geom);
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
