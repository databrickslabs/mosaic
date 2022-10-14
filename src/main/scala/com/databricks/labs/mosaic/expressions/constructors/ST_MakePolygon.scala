package com.databricks.labs.mosaic.expressions.constructors

import com.databricks.labs.mosaic.core.types.InternalGeometryType
import com.databricks.labs.mosaic.core.types.model.{GeometryTypeEnum, InternalGeometry}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription, ExpressionInfo, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType

@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2) - Creates a new Polygon geometry from: " +
      " a closed LineString representing the polygon boundary and" +
      " an Array of closed LineStrings representing holes in the polygon.",
  examples = """
    Examples:
      > SELECT _FUNC_(A, B);
  """,
  since = "1.0"
)
case class ST_MakePolygon(boundaryRing: Expression, holeRingArray: Expression)
    extends BinaryExpression
      with NullIntolerant
      with CodegenFallback {

    override def left: Expression = boundaryRing

    override def right: Expression = holeRingArray

    override def dataType: DataType = InternalGeometryType

    override def nullSafeEval(input1: Any, input2: Any): Any = {
        val ringGeom = InternalGeometry(input1.asInstanceOf[InternalRow])
        val holeGeoms = input2
            .asInstanceOf[ArrayData]
            .toObjectArray(InternalGeometryType)
            .map(g => InternalGeometry(g.asInstanceOf[InternalRow]))

        val polygon =
            new InternalGeometry(GeometryTypeEnum.POLYGON.id, ringGeom.srid, ringGeom.boundaries, Array(holeGeoms.map(_.boundaries.head)))
        polygon.serialize
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
        val res = ST_MakePolygon(asArray(0), asArray(1))
        res.copyTagsFrom(this)
        res
    }

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
        copy(boundaryRing = newLeft, holeRingArray = newRight)

}

object ST_MakePolygon {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_MakePolygon].getCanonicalName,
          db.orNull,
          "st_makepolygon",
          """
            |    _FUNC_(expr1, expr2) - Creates a new Polygon geometry from:
            |         a closed LineString representing the polygon boundary and
            |         an Array of closed LineStrings representing holes in the polygon.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(A, B);
            |  """.stripMargin,
          "",
          "collection_funcs",
          "1.0",
          "",
          "built-in"
        )

}
