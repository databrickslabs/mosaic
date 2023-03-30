package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
import com.databricks.labs.mosaic.core.types.{HexType, InternalGeometryType}
import com.databricks.labs.mosaic.core.Mosaic
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionInfo, NullIntolerant, TernaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

case class GeometryKRing(
    geom: Expression,
    resolution: Expression,
    k: Expression,
    indexSystem: IndexSystem,
    geometryAPIName: String
) extends TernaryExpression
      with ExpectsInputTypes
      with NullIntolerant
      with CodegenFallback {

    val geometryAPI: GeometryAPI = GeometryAPI(geometryAPIName)

    // noinspection DuplicatedCode
    override def inputTypes: Seq[DataType] = {
        if (
          !Seq(BinaryType, StringType, HexType, InternalGeometryType).contains(first.dataType) ||
          !Seq(IntegerType, StringType).contains(second.dataType) ||
          third.dataType != IntegerType
        ) {
            throw new Error(s"Not supported data type: (${first.dataType}, ${second.dataType}, ${third.dataType}.")
        } else {
            Seq(first.dataType, second.dataType, third.dataType)
        }
    }

    override def first: Expression = geom

    override def second: Expression = resolution

    override def third: Expression = k

    /** Expression output DataType. */
    override def dataType: DataType = ArrayType(indexSystem.getCellIdDataType)

    override def toString: String = s"grid_geometrykring($geom, $k)"

    /** Overridden to ensure [[Expression.sql]] is properly formatted. */
    override def prettyName: String = "grid_geometrykring"

    /**
      * Generates a set of indices corresponding to kring call over the input
      * cell id.
      *
      * @param input1
      *   Any instance containing the cell id.
      * @param input2
      *   Any instance containing the k.
      * @return
      *   A set of indices.
      */
    // noinspection DuplicatedCode
    override def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
        val geometry = geometryAPI.geometry(input1, first.dataType)
        val resolution: Int = indexSystem.getResolution(input2)
        val k: Int = input3.asInstanceOf[Int]

        val kRing = Mosaic.geometryKRing(geometry, resolution, k, indexSystem, geometryAPI)

        val formatted = kRing.map(indexSystem.serializeCellId)
        val serialized = ArrayData.toArrayData(formatted.toArray)
        serialized
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(3).map(_.asInstanceOf[Expression])
        val res = GeometryKRing(asArray(0), asArray(1), asArray(2), indexSystem, geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override def withNewChildrenInternal(
        newFirst: Expression,
        newSecond: Expression,
        newThird: Expression
    ): Expression = {
        copy(newFirst, newSecond, newThird)
    }

}

object GeometryKRing {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
            classOf[GeometryKRing].getCanonicalName,
            db.orNull,
            "grid_cellkring",
            "_FUNC_(cellId, res, k) - Returns the k ring for a given geometry." +
                "The k ring is the set of cells that are within the k distance of the input" +
                "geometry boundary. The k ring is produced using grid_tessellation. For each" +
                "border cell, the k ring is computed and the set union of all k rings is returned.",
            "",
            """
              |    Examples:
              |      > SELECT _FUNC_(a, b, c);
              |       [622236721348804607, 622236721274716159, ...]
              |  """.stripMargin,
            "",
            "collection_funcs",
            "1.0",
            "",
            "built-in"
        )

}