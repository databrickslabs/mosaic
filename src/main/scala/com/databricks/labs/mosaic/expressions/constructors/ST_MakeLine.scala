package com.databricks.labs.mosaic.expressions.constructors

import com.databricks.labs.mosaic.core.types.InternalGeometryType
import com.databricks.labs.mosaic.core.types.model.{GeometryTypeEnum, InternalGeometry}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ExpressionInfo, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType

@ExpressionDescription(
  usage = "_FUNC_(expr1) - Creates a new LineString geometry from an Array of Point, MultiPoint, LineString or MultiLineString geometries.",
  examples = """
    Examples:
      > SELECT _FUNC_(A);
  """,
  since = "1.0"
)
case class ST_MakeLine(geoms: Expression) extends UnaryExpression with NullIntolerant with CodegenFallback {

    override def child: Expression = geoms

    override def dataType: DataType = InternalGeometryType

    override def eval(input: InternalRow): Any = {
        val geomArray = geoms.eval(input).asInstanceOf[ArrayData].toObjectArray(InternalGeometryType)
        val internalGeoms = geomArray.map(g => InternalGeometry(g.asInstanceOf[InternalRow]))
        val outputGeom = internalGeoms.reduce(reduceGeoms)
        outputGeom.serialize

    }

    def reduceGeoms(leftGeom: InternalGeometry, rightGeom: InternalGeometry): InternalGeometry =
        new InternalGeometry(
          GeometryTypeEnum.LINESTRING.id,
          Array(leftGeom.boundaries.flatMap(_.toList) ++ rightGeom.boundaries.flatMap(_.toList)),
          Array(Array())
        )

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
        val res = ST_MakeLine(asArray.head)
        res.copyTagsFrom(this)
        res
    }

    override protected def withNewChildInternal(newChild: Expression): Expression = copy(geoms = newChild)

}

object ST_MakeLine {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_MakeLine].getCanonicalName,
          db.orNull,
          "st_makeline",
          """
            |    _FUNC_(expr1) - Creates a new LineString geometry from an Array of Point, MultiPoint, LineString or MultiLineString geometries.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(A);
            |  """.stripMargin,
          "",
          "collection_funcs",
          "1.0",
          "",
          "built-in"
        )

}
