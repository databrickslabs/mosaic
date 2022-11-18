package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystemID
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ExpressionInfo, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

@ExpressionDescription(
    usage = "_FUNC_(indexID, indexSystem) - Returns the geometry representing the centroid of the index.",
    examples = """
    Examples:
      > SELECT _FUNC_(a, 'H3');
      0001100100100.....001010 // WKB
  """,
    since = "1.0"
)
case class GridCenterAsWKB(indexID: Expression, indexSystemName: String, geometryAPIName: String)
    extends UnaryExpression
        with NullIntolerant
        with CodegenFallback {

    /** Expression output DataType. */
    // Return WKB, if other type is required call ConvertTO
    override def dataType: DataType = BinaryType

    override def toString: String = s"grid_centeraswkb($indexID)"

    /** Overridden to ensure [[Expression.sql]] is properly formatted. */
    override def prettyName: String = "grid_centeraswkb"

    /**
     * Returns the geometry representing the centroid of the index.
     *
     * @param input1
     *   Any instance containing the ID of the index.
     * @return
     *   WKB representation of the centroid of the index provided.
     */
    override def nullSafeEval(input1: Any): Any = {
        val indexSystem = IndexSystemID.getIndexSystem(IndexSystemID(indexSystemName))
        val geometryAPI = GeometryAPI(geometryAPIName)
        val indexCentroid = indexSystem.GridCenterAsWKB(input1.asInstanceOf[Long], geometryAPI)
        indexCentroid.toWKB
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val arg1 = newArgs.head.asInstanceOf[Expression]
        val res = GridCenterAsWKB(indexID, indexSystemName, geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override def child: Expression = indexID

    override protected def withNewChildInternal(newChild: Expression): Expression = copy(indexID = newChild)

}

object GridCenterAsWKB {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
            classOf[GridCenterAsWKB].getCanonicalName,
            db.orNull,
            "grid_centeraswkb",
            """
              |    _FUNC_(indexID) - Returns the geometry representing the centroid of the index.
            """.stripMargin,
            "",
            """
              |    Examples:
              |      > SELECT _FUNC_(a);
              |        0001100100100.....001010 // WKB
              |  """.stripMargin,
            "",
            "misc_funcs",
            "1.0",
            "",
            "built-in"
        )
}
