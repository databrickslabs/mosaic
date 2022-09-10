package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemID}
import com.databricks.labs.mosaic.core.types.ChipType
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{BinaryType, DataType}

case class ST_IntersectionMosaic(leftGeom: Expression, rightGeom: Expression, geometryAPIName: String, indexSystemName: String)
    extends BinaryExpression
      with NullIntolerant
      with CodegenFallback {

    lazy val geometryAPI: GeometryAPI = GeometryAPI.apply(geometryAPIName)
    lazy val indexSystem: IndexSystem = IndexSystemID.getIndexSystem(IndexSystemID.apply(indexSystemName))

    override def dataType: DataType = BinaryType

    // noinspection DuplicatedCode
    override def nullSafeEval(input1: Any, input2: Any): Any = {
        val leftChips = getChips(input1)
        val rightChips = getChips(input2)

        val matches = for {
            (f1, k, v1) <- leftChips
            (f2, `k`, v2) <- rightChips
        } yield (k, (f1, f2), (v1, v2))

        val intersectionGeom = matches.foldLeft(Option.empty[MosaicGeometry]) { case (result, (cellId, flags, geoms)) =>
            val geomIncrement =
                if (flags._1 && flags._2) {
                    indexSystem.indexToGeometry(cellId, geometryAPI)
                } else if (flags._1) {
                    geometryAPI.geometry(geoms._2, "WKB")
                } else if (flags._2) {
                    geometryAPI.geometry(geoms._1, "WKB")
                } else {
                    val leftChipGeom = geometryAPI.geometry(geoms._1, "WKB")
                    val rightChipGeom = geometryAPI.geometry(geoms._2, "WKB")
                    leftChipGeom.intersection(rightChipGeom)
                }
            result match {
                case None          => Some(geomIncrement)
                case Some(current) => Some(current.union(geomIncrement))
            }
        }

        intersectionGeom match {
            case None             => geometryAPI.serialize(geometryAPI.geometry("POLYGON EMPTY", "WKT"), BinaryType)
            case Some(resultGeom) => geometryAPI.serialize(resultGeom, BinaryType)
        }
    }

    private def getChips(input: Any): Seq[(Boolean, Long, Array[Byte])] = {
        input.asInstanceOf[ArrayData].toArray[InternalRow](ChipType).map(row => (row.getBoolean(0), row.getLong(1), row.getBinary(2)))
    }

    override def left: Expression = leftGeom

    override def right: Expression = rightGeom

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
        val res = ST_IntersectionMosaic(asArray(0), asArray(1), geometryAPIName, indexSystemName)
        res.copyTagsFrom(this)
        res
    }

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
        copy(leftGeom = newLeft, rightGeom = newRight)

}

object ST_IntersectionMosaic {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_IntersectionMosaic].getCanonicalName,
          db.orNull,
          "st_intersection_mosaic",
          """
            |    _FUNC_(expr1, expr2) - Returns the intersection of the two geometries.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, b);
            |        POLYGON(...)
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )

}
