package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystemID

import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ExpressionInfo, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = "_FUNC_(indexID, indexSystem) - Returns the geometry representing the index.",
  examples = """
    Examples:
      > SELECT _FUNC_(a, 'H3');
      0001100100100.....001010 // WKB
  """,
  since = "1.0"
)
case class MosaicIndexToGeometry(indexID: Expression, indexSystemName: String, geometryAPIName: String)
    extends UnaryExpression
      with NullIntolerant
      with CodegenFallback {

    /** Expression output DataType. */
    // Return WKB, if other type is required call ConvertTO
    override def dataType: DataType = BinaryType

    override def toString: String = s"mosaic_index_to_geometry($indexID)"

    /** Overridden to ensure [[Expression.sql]] is properly formatted. */
    override def prettyName: String = "mosaic_index_to_geometry"

    /**
      * Computes the H3 index corresponding to the provided lat and long
      * coordinates.
      *
      * @param input1
      *   Any instance containing the ID of the index.
      * @return
      *   H3 index id in Long.
      */
    override def nullSafeEval(input1: Any): Any = {
        val indexSystem = IndexSystemID.getIndexSystem(IndexSystemID(indexSystemName))
        val geometryAPI = GeometryAPI(geometryAPIName)
        val indexGeometry = indexSystem.indexToGeometry(input1.asInstanceOf[Long], geometryAPI)
        indexGeometry.toWKB
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val arg1 = newArgs.head.asInstanceOf[Expression]
        val res = MosaicIndexToGeometry(arg1, indexSystemName, geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override def child: Expression = indexID

    override protected def withNewChildInternal(newChild: Expression): Expression = copy(indexID = newChild)

}

object MosaicIndexToGeometry {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[MosaicIndexToGeometry].getCanonicalName,
          db.orNull,
          "mosaic_index_to_geometry",
          """
            |    _FUNC_(indexID, indexSystem) - Returns the geometry representing the index.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, 'H3');
            |        0001100100100.....001010 // WKB
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )
}
