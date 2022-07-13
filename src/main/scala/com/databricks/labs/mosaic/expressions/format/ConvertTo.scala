package com.databricks.labs.mosaic.expressions.format

import java.util.Locale

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.types._

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ExpressionInfo, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types._

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
case class ConvertTo(inGeometry: Expression, outDataType: String, geometryAPIName: String) extends UnaryExpression with NullIntolerant {

    /**
      * Ensure that the expression is called only for compatible pairing of
      * input geometry and requested output data type. Data type is provided as
      * a string keyword. Input geometry type is extracted from the expression
      * input.
      *
      * @return
      *   An instance of [[TypeCheckResult]] indicating success or a failure.
      */
    override def checkInputDataTypes(): TypeCheckResult =
        (inGeometry.dataType, outDataType.toUpperCase(Locale.ROOT)) match {
            case (BinaryType, "WKT")               => TypeCheckResult.TypeCheckSuccess
            case (BinaryType, "WKB")               => TypeCheckResult.TypeCheckSuccess
            case (BinaryType, "COORDS")            => TypeCheckResult.TypeCheckSuccess
            case (BinaryType, "HEX")               => TypeCheckResult.TypeCheckSuccess
            case (BinaryType, "GEOJSON")           => TypeCheckResult.TypeCheckSuccess
            case (BinaryType, "KRYO")              => TypeCheckResult.TypeCheckSuccess
            case (StringType, "WKT")               => TypeCheckResult.TypeCheckSuccess
            case (StringType, "WKB")               => TypeCheckResult.TypeCheckSuccess
            case (StringType, "COORDS")            => TypeCheckResult.TypeCheckSuccess
            case (StringType, "HEX")               => TypeCheckResult.TypeCheckSuccess
            case (StringType, "GEOJSON")           => TypeCheckResult.TypeCheckSuccess
            case (StringType, "KRYO")              => TypeCheckResult.TypeCheckSuccess
            case (HexType, "WKT")                  => TypeCheckResult.TypeCheckSuccess
            case (HexType, "WKB")                  => TypeCheckResult.TypeCheckSuccess
            case (HexType, "COORDS")               => TypeCheckResult.TypeCheckSuccess
            case (HexType, "HEX")                  => TypeCheckResult.TypeCheckSuccess
            case (HexType, "GEOJSON")              => TypeCheckResult.TypeCheckSuccess
            case (HexType, "KRYO")                 => TypeCheckResult.TypeCheckSuccess
            case (JSONType, "WKT")                 => TypeCheckResult.TypeCheckSuccess
            case (JSONType, "WKB")                 => TypeCheckResult.TypeCheckSuccess
            case (JSONType, "HEX")                 => TypeCheckResult.TypeCheckSuccess
            case (JSONType, "COORDS")              => TypeCheckResult.TypeCheckSuccess
            case (JSONType, "GEOJSON")             => TypeCheckResult.TypeCheckSuccess
            case (JSONType, "KRYO")                => TypeCheckResult.TypeCheckSuccess
            case (InternalGeometryType, "WKB")     => TypeCheckResult.TypeCheckSuccess
            case (InternalGeometryType, "WKT")     => TypeCheckResult.TypeCheckSuccess
            case (InternalGeometryType, "COORDS")  => TypeCheckResult.TypeCheckSuccess
            case (InternalGeometryType, "HEX")     => TypeCheckResult.TypeCheckSuccess
            case (InternalGeometryType, "GEOJSON") => TypeCheckResult.TypeCheckSuccess
            case (InternalGeometryType, "KRYO")    => TypeCheckResult.TypeCheckSuccess
            case (KryoType, "WKB")                 => TypeCheckResult.TypeCheckSuccess
            case (KryoType, "WKT")                 => TypeCheckResult.TypeCheckSuccess
            case (KryoType, "COORDS")              => TypeCheckResult.TypeCheckSuccess
            case (KryoType, "HEX")                 => TypeCheckResult.TypeCheckSuccess
            case (KryoType, "GEOJSON")             => TypeCheckResult.TypeCheckSuccess
            case (KryoType, "KRYO")                => TypeCheckResult.TypeCheckSuccess
            case _                                 => TypeCheckResult.TypeCheckFailure(
                  s"Cannot convert from ${inGeometry.dataType.sql} to $dataType"
                )
        }

    /** Expression output DataType. */
    override def dataType: DataType =
        outDataType.toUpperCase(Locale.ROOT) match {
            case "WKT"     => StringType
            case "WKB"     => BinaryType
            case "HEX"     => HexType
            case "COORDS"  => InternalGeometryType
            case "GEOJSON" => StringType
            case "KRYO"    => KryoType
        }

    override def toString: String = s"convert_to($inGeometry, $outDataType)"

    /** Overridden to ensure [[Expression.sql]] is properly formatted. */
    override def prettyName: String = "convert_to"

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

        val geometryAPI = GeometryAPI(geometryAPIName)
        val geometry = geometryAPI.geometry(input, inGeometry.dataType)
        geometryAPI.serialize(geometry, outDataType)
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val res = ConvertTo(
          newArgs(0).asInstanceOf[Expression],
          outDataType,
          geometryAPIName
        )
        res.copyTagsFrom(this)
        res
    }

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
        val geometryAPI = GeometryAPI.apply(geometryAPIName)
        ConvertToCodeGen.doCodeGenESRI(
          ctx,
          ev,
          nullSafeCodeGen,
          child.dataType,
          outDataType.toUpperCase(Locale.ROOT),
          geometryAPI
        )
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
