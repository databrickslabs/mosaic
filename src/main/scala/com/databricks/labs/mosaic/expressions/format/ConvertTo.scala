package com.databricks.labs.mosaic.expressions.format

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.types._
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ExpressionInfo, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types._

import java.util.Locale

@ExpressionDescription(
  usage = "_FUNC_(expr1, dataType) - Converts expr1 to the specified data type.",
  examples = """
    Examples:
      > SELECT _FUNC_(a, 'hex');
       {"00001005FA...00A"}
      > SELECT _FUNC_(a, 'wkt');
      "POLYGON ((...))"
  """,
  since = "1.0"
)
case class ConvertTo(inGeometry: Expression, outDataType: String, geometryAPIName: String,
                     functionName: Option[String] = None)
    extends UnaryExpression with NullIntolerant {

    /**
      * Ensure that the expression is called only for compatible pairing of
      * input geometry and requested output data type. Data type is provided as
      * a string keyword. Input geometry type is extracted from the expression
      * input.
      *
      * @return
      *   An instance of [[TypeCheckResult]] indicating success or a failure.
      */
    override def checkInputDataTypes(): TypeCheckResult = {

        val inputTypes = Seq(BinaryType, StringType, HexType, JSONType, InternalGeometryType)
        val outputDataTypes = Seq("WKT", "WKB", "COORDS", "HEX", "GEOJSON", "JSONOBJECT")

        if (inputTypes.contains(inGeometry.dataType) && outputDataTypes.contains(outDataType.toUpperCase(Locale.ROOT))) {
            TypeCheckResult.TypeCheckSuccess
        } else {
            TypeCheckResult.TypeCheckFailure(
              s"Cannot convert from ${inGeometry.dataType.sql} to $dataType"
            )
        }
    }

    /** Expression output DataType. */
    override def dataType: DataType =
        outDataType.toUpperCase(Locale.ROOT) match {
            case "WKT"        => StringType
            case "WKB"        => BinaryType
            case "HEX"        => HexType
            case "COORDS"     => InternalGeometryType
            case "GEOJSON"    => StringType
            case "JSONOBJECT" => JSONType
            case _         => throw new Error(s"Data type not supported: $outDataType")
        }

    override def toString: String = s"convert_to($inGeometry, $outDataType)"

    /** Overridden to ensure [[Expression.sql]] is properly formatted. */
    override def prettyName: String = functionName.getOrElse("convert_to")

    /**
      * Depending on the input data type and output data type keyword a specific
      * conversion function will be executed after the case matching has been
      * evaluated.
      *
      * @param input
      *   Type-erased input data that will be casted to a correct data type
      *   based on the case matching and the called function.
      * @return
      *   A converted representation of the input geometry.
      */
    override def nullSafeEval(input: Any): Any = {
        if (inGeometry.dataType.simpleString == outDataType) {
            input
        } else {
            val geometryAPI = GeometryAPI(geometryAPIName)
            val geometry = geometryAPI.geometry(input, inGeometry.dataType)
            geometryAPI.serialize(geometry, outDataType)
        }
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val res = ConvertTo(
          newArgs(0).asInstanceOf[Expression],
          outDataType,
          geometryAPIName,
          functionName
        )
        res.copyTagsFrom(this)
        res
    }

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
        val geometryAPI = GeometryAPI.apply(geometryAPIName)
        ConvertToCodeGen.doCodeGen(
          ctx,
          ev,
          nullSafeCodeGen,
          child.dataType,
          outDataType.toUpperCase(Locale.ROOT),
          geometryAPI
        )
    }

    override def sql: String ={
        val childrenSQL = children.map(_.sql).mkString(", ")
        s"$prettyName($childrenSQL)"
    }

    override def child: Expression = inGeometry

    override protected def withNewChildInternal(newChild: Expression): Expression = copy(inGeometry = newChild)

}

object ConvertTo {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String], name: String): ExpressionInfo =
        new ExpressionInfo(
          classOf[ConvertTo].getCanonicalName,
          db.orNull,
          name,
          "_FUNC_(col1) - Wraps the column in a fixed struct for type inference.",
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, 'hex');
            |       {"00001005FA...00A"}
            |      > SELECT _FUNC_(a, 'wkt');
            |      "POLYGON ((...))"
            |  """.stripMargin,
          "",
          "struct_funcs",
          "1.0",
          "",
          "built-in"
        )

}
