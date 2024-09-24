package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
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
case class Polyfill(geom: Expression, resolution: Expression, indexSystem: IndexSystem, geometryAPIName: String)
    extends BinaryExpression
      with ExpectsInputTypes
      with NullIntolerant
      with CodegenFallback {

    val geometryAPI: GeometryAPI = GeometryAPI(geometryAPIName)

    // noinspection DuplicatedCode
    override def inputTypes: Seq[DataType] = {
        if (
          !Seq(BinaryType, StringType, HexType, InternalGeometryType).contains(geom.dataType) ||
          !Seq(IntegerType, StringType).contains(resolution.dataType)
        ) {
            throw new Error(s"Not supported data type: (${geom.dataType}, ${resolution.dataType}.")
        } else {
            Seq(geom.dataType, resolution.dataType)
        }
    }

    /** Expression output DataType. */
    override def dataType: DataType = ArrayType(indexSystem.getCellIdDataType)

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
    override def nullSafeEval(input1: Any, input2: Any): Any = {
        val resolutionVal: Int = indexSystem.getResolution(input2)

        val geometry = geometryAPI.geometry(input1, geom.dataType)
        val indices = indexSystem.polyfill(geometry, resolutionVal, geometryAPI)

        val formatted = indices.map(indexSystem.formatCellId)
        val serialized = ArrayData.toArrayData(formatted.toArray)
        serialized
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(3).map(_.asInstanceOf[Expression])
        val res = Polyfill(asArray(0), asArray(1), indexSystem, geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override def left: Expression = geom

    override def right: Expression = resolution

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Polyfill =
        copy(geom = newLeft, resolution = newRight)

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
