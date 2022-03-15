package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.Mosaic
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{H3IndexSystem, IndexSystemID}
import com.databricks.labs.mosaic.core.types._
import org.locationtech.jts.geom.Geometry

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
    BinaryExpression,
    ExpectsInputTypes,
    Expression,
    ExpressionDescription,
    ExpressionInfo,
    NullIntolerant
}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = "_FUNC_(geometry, resolution) - Returns the 2 set representation of geometry at resolution.",
  examples = """
    Examples:
      > SELECT _FUNC_(a, b);
       [{index_id, is_border, chip_geom}, {index_id, is_border, chip_geom}, ..., {index_id, is_border, chip_geom}]
  """,
  since = "1.0"
)
case class MosaicFill(geom: Expression, resolution: Expression, indexSystemName: String, geometryAPIName: String)
    extends BinaryExpression
      with ExpectsInputTypes
      with NullIntolerant
      with CodegenFallback {

    // noinspection DuplicatedCode
    override def inputTypes: Seq[DataType] =
        (left.dataType, right.dataType) match {
            case (BinaryType, IntegerType)           => Seq(BinaryType, IntegerType)
            case (StringType, IntegerType)           => Seq(StringType, IntegerType)
            case (HexType, IntegerType)              => Seq(HexType, IntegerType)
            case (InternalGeometryType, IntegerType) => Seq(InternalGeometryType, IntegerType)
            case _ => throw new IllegalArgumentException(s"Not supported data type: (${left.dataType}, ${right.dataType}).")
        }

    override def right: Expression = resolution

    /** Expression output DataType. */
    override def dataType: DataType = MosaicType

    override def toString: String = s"h3_mosaicfill($geom, $resolution)"

    /** Overridden to ensure [[Expression.sql]] is properly formatted. */
    override def prettyName: String = "h3_mosaicfill"

    /**
      * Type-wise differences in evaluation are only present on the input data
      * conversion to a [[Geometry]]. The rest of the evaluation is agnostic to
      * the input data type. The evaluation generates a set of core indices that
      * are fully contained by the input [[Geometry]] and a set of border
      * indices that are partially contained by the input [[Geometry]].
      *
      * @param input1
      *   Any instance containing the geometry.
      * @param input2
      *   Any instance containing the resolution
      * @return
      *   A set of serialized
      *   [[com.databricks.labs.mosaic.core.types.model.MosaicChip]].
      */
    // noinspection DuplicatedCode
    override def nullSafeEval(input1: Any, input2: Any): Any = {
        val resolution: Int = H3IndexSystem.getResolution(input2)

        val indexSystem = IndexSystemID.getIndexSystem(IndexSystemID(indexSystemName))
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geometry = geometryAPI.geometry(input1, left.dataType)
        val chips = Mosaic.mosaicFill(geometry, resolution, indexSystem, geometryAPI)

        val serialized = InternalRow.fromSeq(
          Seq(
            ArrayData.toArrayData(chips.map(_.serialize))
          )
        )

        serialized
    }

    override def left: Expression = geom

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
        val res = MosaicFill(asArray(0), asArray(1), indexSystemName, geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
        copy(geom = newLeft, resolution = newRight)

}

object MosaicFill {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[IndexGeometry].getCanonicalName,
          db.orNull,
          "mosaic_fill",
          """
            |    _FUNC_(geometry, resolution) - Returns the 2 set representation of geometry at resolution.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, b);
            |        [{index_id, is_border, chip_geom}, {index_id, is_border, chip_geom}, ..., {index_id, is_border, chip_geom}]
            |  """.stripMargin,
          "",
          "collection_funcs",
          "1.0",
          "",
          "built-in"
        )

}
