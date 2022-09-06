package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystemID
import com.databricks.labs.mosaic.core.types.{HexType, InternalGeometryType}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = "_FUNC_(geometry, resolution) - Returns the 1 set representation of geometry at resolution.",
  examples = """
    Examples:
      > SELECT _FUNC_(a, b);
       [622236721348804607, 622236721274716159, ...]
  """,
  since = "1.0"
)
case class Polyfill(geom: Expression, resolution: Expression, idAsLong: Expression, indexSystemName: String, geometryAPIName: String)
    extends TernaryExpression
      with ExpectsInputTypes
      with NullIntolerant
      with CodegenFallback {

    // noinspection DuplicatedCode
    override def inputTypes: Seq[DataType] =
        (geom.dataType, resolution.dataType, idAsLong.dataType) match {
            case (BinaryType, IntegerType, BooleanType)           => Seq(BinaryType, IntegerType, BooleanType)
            case (StringType, IntegerType, BooleanType)           => Seq(StringType, IntegerType, BooleanType)
            case (HexType, IntegerType, BooleanType)              => Seq(HexType, IntegerType, BooleanType)
            case (InternalGeometryType, IntegerType, BooleanType) => Seq(InternalGeometryType, IntegerType, BooleanType)
            case (BinaryType, StringType, BooleanType)            => Seq(BinaryType, StringType, BooleanType)
            case (StringType, StringType, BooleanType)            => Seq(StringType, StringType, BooleanType)
            case (HexType, StringType, BooleanType)               => Seq(HexType, StringType, BooleanType)
            case (InternalGeometryType, StringType, BooleanType)  => Seq(InternalGeometryType, StringType, BooleanType)
            case _ => throw new Error(s"Not supported data type: (${geom.dataType}, ${resolution.dataType}, ${idAsLong.dataType}).")
        }

    /** Expression output DataType. */
    override def dataType: DataType =
        idAsLong match {
            case Literal(f: Boolean, BooleanType) => if (f) ArrayType(LongType) else ArrayType(StringType)
            case _                                => throw new Error("idAsLong has to be Boolean type.")
        }

    override def toString: String = s"grid_polyfill($geom, $resolution)"

    /** Overridden to ensure [[Expression.sql]] is properly formatted. */
    override def prettyName: String = "grid_polyfill"

    /**
      * Generates a set of indices corresponding to polyfill call over the input
      * geometry.
      *
      * @param input1
      *   Any instance containing the geometry.
      * @param input2
      *   Any instance containing the resolution.
      * @return
      *   A set of indices.
      */
    // noinspection DuplicatedCode
    override def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
        val indexSystem = IndexSystemID.getIndexSystem(IndexSystemID(indexSystemName))
        val resolutionVal: Int = indexSystem.getResolution(input2)
        val idAsLongVal = input3.asInstanceOf[Boolean]
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geometry = geometryAPI.geometry(input1, geom.dataType)
        val indices = indexSystem.polyfill(geometry, resolutionVal, Some(geometryAPI))
        val formatted = if (idAsLongVal) indices else indices.map(indexSystem.format)
        val serialized = ArrayData.toArrayData(formatted.toArray)
        serialized
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(3).map(_.asInstanceOf[Expression])
        val res = Polyfill(asArray(0), asArray(1), asArray(2), indexSystemName, geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override def first: Expression = geom

    override def second: Expression = resolution

    override def third: Expression = idAsLong

    override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression =
        copy(newFirst, newSecond, newThird)

}

object Polyfill {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[Polyfill].getCanonicalName,
          db.orNull,
          "grid_polyfill",
          """
            |    _FUNC_(geometry, resolution) - Returns the 1 set representation of geometry at resolution.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, b);
            |        [622236721348804607, 622236721274716159, ...]
            |  """.stripMargin,
          "",
          "collection_funcs",
          "1.0",
          "",
          "built-in"
        )

}
