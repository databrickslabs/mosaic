package com.databricks.labs.mosaic.expressions.constructors

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.types.InternalGeometryType
import com.databricks.labs.mosaic.core.types.model.{GeometryTypeEnum, InternalGeometry}
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.LINESTRING

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ExpressionInfo, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType}

@ExpressionDescription(
  usage = "_FUNC_(expr1) - Creates a new LineString geometry from an Array of Point, MultiPoint, LineString or MultiLineString geometries.",
  examples = """
    Examples:
      > SELECT _FUNC_(A);
  """,
  since = "1.0"
)
case class ST_MakeLine(geoms: Expression, geometryAPIName: String) extends UnaryExpression with CodegenFallback {

    override def child: Expression = geoms

    override def nullable: Boolean = true

    override def eval(input: InternalRow): Any = {
        val evaluated = geoms.eval(input)
        if (Option(evaluated).isEmpty) {
            null
        } else {
            val dataArray = evaluated.asInstanceOf[ArrayData]
            val n = dataArray.numElements()
            val anyNull = (for (i <- 0 until n) yield dataArray.isNullAt(i)).exists(identity)
            if (anyNull) {
                null
            } else {
                val geometryAPI = GeometryAPI(geometryAPIName)
                val geomArray = evaluated.asInstanceOf[ArrayData].toObjectArray(dataType)
                val geomPieces = geomArray.map(geometryAPI.geometry(_, dataType))
                val internalGeoms = geomPieces
                    .map(geometryAPI.serialize(_, InternalGeometryType))
                    .map(geom => InternalGeometry(geom.asInstanceOf[InternalRow]))
                val outputGeom = internalGeoms.reduce(reduceGeoms)
                val result = geometryAPI.geometry(outputGeom.serialize, InternalGeometryType)
                geometryAPI.serialize(result, dataType)
            }
        }
    }

    override def dataType: DataType = geoms.dataType.asInstanceOf[ArrayType].elementType

    def reduceGeoms(leftGeom: InternalGeometry, rightGeom: InternalGeometry): InternalGeometry =
        new InternalGeometry(
          GeometryTypeEnum.LINESTRING.id,
          leftGeom.srid,
          Array(leftGeom.boundaries.flatMap(_.toList) ++ rightGeom.boundaries.flatMap(_.toList)),
          Array(Array())
        )

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
        val res = ST_MakeLine(asArray.head, geometryAPIName)
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
