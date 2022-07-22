package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.Mosaic
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystemID
import com.databricks.labs.mosaic.core.types._
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.{LINESTRING, MULTILINESTRING}
import org.locationtech.jts.geom.Geometry

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
    ExpectsInputTypes,
    Expression,
    ExpressionDescription,
    ExpressionInfo,
    NullIntolerant,
    TernaryExpression
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
case class MosaicFill(geom: Expression, resolution: Expression, keepCoreGeom: Expression, indexSystemName: String, geometryAPIName: String)
    extends TernaryExpression
      with ExpectsInputTypes
      with NullIntolerant
      with CodegenFallback {

    // noinspection DuplicatedCode
    override def inputTypes: Seq[DataType] =
        (first.dataType, second.dataType, third.dataType) match {
            case (BinaryType, IntegerType, BooleanType)           => Seq(BinaryType, IntegerType, BooleanType)
            case (StringType, IntegerType, BooleanType)           => Seq(StringType, IntegerType, BooleanType)
            case (HexType, IntegerType, BooleanType)              => Seq(HexType, IntegerType, BooleanType)
            case (InternalGeometryType, IntegerType, BooleanType) => Seq(InternalGeometryType, IntegerType, BooleanType)
            case _                                                =>
                throw new Error(s"Not supported data type: (${first.dataType}, ${second.dataType}, ${third.dataType}).")
        }

    override def second: Expression = resolution

    override def third: Expression = keepCoreGeom

    /** Expression output DataType. */
    override def dataType: DataType = MosaicType

    override def toString: String = s"mosaicfill($geom, $resolution, $keepCoreGeom)"

    /** Overridden to ensure [[Expression.sql]] is properly formatted. */
    override def prettyName: String = "mosaicfill"

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
      * @param input3
      *   Any instance defining if core chips should be geometries or nulls
      * @return
      *   A set of serialized
      *   [[com.databricks.labs.mosaic.core.types.model.MosaicChip]].
      */
    // noinspection DuplicatedCode
    override def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
        val indexSystem = IndexSystemID.getIndexSystem(IndexSystemID(indexSystemName))
        val resolution: Int = indexSystem.getResolution(input2)
        val keepCoreGeom: Boolean = input3.asInstanceOf[Boolean]

        val geometryAPI = GeometryAPI(geometryAPIName)
        val geometry = geometryAPI.geometry(input1, first.dataType)

        val chips = GeometryTypeEnum.fromString(geometry.getGeometryType) match {
            case LINESTRING      => Mosaic.lineFill(geometry, resolution, indexSystem, geometryAPI)
            case MULTILINESTRING => Mosaic.lineFill(geometry, resolution, indexSystem, geometryAPI)
            case _               => Mosaic.mosaicFill(geometry, resolution, keepCoreGeom, indexSystem, geometryAPI)
        }

        val serialized = InternalRow.fromSeq(
          Seq(
            ArrayData.toArrayData(chips.map(_.serialize))
          )
        )

        serialized
    }

    override def first: Expression = geom

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(3).map(_.asInstanceOf[Expression])
        val res = MosaicFill(asArray(0), asArray(1), asArray(2), indexSystemName, geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression =
        copy(geom = newFirst, resolution = newSecond, keepCoreGeom = newThird)

}

object MosaicFill {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[IndexGeometry].getCanonicalName,
          db.orNull,
          "mosaicfill",
          """
            |    _FUNC_(geometry, resolution, keepCoreGeom) - Returns the 2 set representation of geometry at resolution.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, b, c);
            |        [{index_id, is_border, chip_geom}, {index_id, is_border, chip_geom}, ..., {index_id, is_border, chip_geom}]
            |  """.stripMargin,
          "",
          "collection_funcs",
          "1.0",
          "",
          "built-in"
        )

}
