package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
import com.databricks.labs.mosaic.core.types.InternalGeometryType
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

@ExpressionDescription(
  usage = "_FUNC_(indexID, indexSystem) - Returns the geometry representing the grid cell.",
  examples = """
    Examples:
      > SELECT _FUNC_(a, 'H3');
      0001100100100.....001010 // WKB
  """,
  since = "1.0"
)
case class IndexGeometry(indexID: Expression, format: Expression, indexSystem: IndexSystem, geometryAPIName: String)
    extends BinaryExpression
      with NullIntolerant
      with CodegenFallback {

    /** Expression output DataType. */
    override def dataType: DataType = {
        val formatExpr = format.asInstanceOf[Literal]
        val formatName = formatExpr.value.asInstanceOf[UTF8String].toString
        formatName match {
            case "WKT"     => StringType
            case "WKB"     => BinaryType
            case "GEOJSON" => StringType
            case "COORDS"  => InternalGeometryType
            case _         => throw new Error(s"Format name can only be 'WKT', 'WKB', 'GEOJSON' or 'COORDS', but $formatName was provided.")
        }
    }

    override def checkInputDataTypes(): TypeCheckResult = {
        (indexID.dataType, format.dataType) match {
            case (LongType, StringType)    => TypeCheckResult.TypeCheckSuccess
            case (IntegerType, StringType)    => TypeCheckResult.TypeCheckSuccess
            case (StringType, StringType)  => TypeCheckResult.TypeCheckSuccess
            case _                         => TypeCheckResult.TypeCheckFailure(
                  s"CellGeometry expression only supports numerical and string cell IDs and StringType for format." +
                      s"But ${indexID.dataType} was provided for ID and ${format.dataType} was provided for format."
                )
        }
    }

    override def toString: String = s"grid_boundaryaswkb($indexID)"

    /** Overridden to ensure [[Expression.sql]] is properly formatted. */
    override def prettyName: String = "grid_boundaryaswkb"

    /**
      * Computes grid cell shape as a polygon from the cell ID
      *
      * @param input1
      *   Any instance containing the ID of the grid cell.
      * @return
      *   A polygon in WKB format representing the cell identified by the
      *   provided ID
      */
    override def nullSafeEval(input1: Any, input2: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        val formatName = input2.asInstanceOf[UTF8String].toString
        val indexGeometry = indexID.dataType match {
            case LongType    => indexSystem.indexToGeometry(input1.asInstanceOf[Long], geometryAPI)
            case IntegerType => indexSystem.indexToGeometry(input1.asInstanceOf[Int], geometryAPI)
            case StringType  => indexSystem.indexToGeometry(indexSystem.parse(input1.asInstanceOf[UTF8String].toString), geometryAPI)
            case _           => throw new Error(s"${indexID.dataType} not supported.")
        }
        geometryAPI.serialize(indexGeometry, formatName)
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val arg1 = newArgs.head.asInstanceOf[Expression]
        val arg2 = newArgs(1).asInstanceOf[Expression]
        val res = IndexGeometry(arg1, arg2, indexSystem, geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override def left: Expression = indexID

    override def right: Expression = format

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = copy(newLeft, newRight)

}

object IndexGeometry {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[IndexGeometry].getCanonicalName,
          db.orNull,
          "grid_boundaryaswkb",
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
