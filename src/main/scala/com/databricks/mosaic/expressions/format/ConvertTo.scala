package com.databricks.mosaic.expressions.format

import com.databricks.mosaic.expressions.format.Conversions._
import com.databricks.mosaic.types.{HexType, InternalGeometryType, JSONType}
import com.databricks.mosaic.types.model.InternalGeometry
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}

@ExpressionDescription(
  usage = "_FUNC_(expr1, dataType) - Converts expr1 to the specified data type.",
  examples =
    """
    Examples:
      > SELECT _FUNC_(a, 'hex');
       {"00001005FA...00A"}
      > SELECT _FUNC_(a, 'wkt');
      "POLYGON ((...))"
  """,
  since = "1.0")
case class ConvertTo(inGeometry: Expression, outDataType: String)
  extends UnaryExpression with NullIntolerant with CodegenFallback {

  /**
   * Ensure that the expression is called only for compatible pairing of
   * input geometry and requested output data type.
   * Data type is provided as a string keyword.
   * Input geometry type is extracted from the expression input.
   * @return An instance of [[TypeCheckResult]] indicating success or a failure.
   */
  override def checkInputDataTypes(): TypeCheckResult =
    (inGeometry.dataType, outDataType.toUpperCase) match {
      case (BinaryType, "WKT") => TypeCheckResult.TypeCheckSuccess
      case (BinaryType, "COORDS") => TypeCheckResult.TypeCheckSuccess
      case (BinaryType, "HEX") => TypeCheckResult.TypeCheckSuccess
      case (BinaryType, "GEOJSON") => TypeCheckResult.TypeCheckSuccess
      case (StringType, "WKB") => TypeCheckResult.TypeCheckSuccess
      case (StringType, "COORDS") => TypeCheckResult.TypeCheckSuccess
      case (StringType, "HEX") => TypeCheckResult.TypeCheckSuccess
      case (StringType, "GEOJSON") => TypeCheckResult.TypeCheckSuccess
      case (dt, "WKT") if dt.sql == HexType.sql => TypeCheckResult.TypeCheckSuccess
      case (dt, "WKB") if dt.sql == HexType.sql => TypeCheckResult.TypeCheckSuccess
      case (dt, "COORDS") if dt.sql == HexType.sql => TypeCheckResult.TypeCheckSuccess
      case (dt, "GEOJSON") if dt.sql == HexType.sql => TypeCheckResult.TypeCheckSuccess
      case (dt, "WKT") if dt.sql == JSONType.sql => TypeCheckResult.TypeCheckSuccess
      case (dt, "WKB") if dt.sql == JSONType.sql => TypeCheckResult.TypeCheckSuccess
      case (dt, "HEX") if dt.sql == JSONType.sql => TypeCheckResult.TypeCheckSuccess
      case (dt, "COORDS") if dt.sql == JSONType.sql => TypeCheckResult.TypeCheckSuccess
      case (dt, "WKB") if dt.sql == InternalGeometryType.sql => TypeCheckResult.TypeCheckSuccess
      case (dt, "WKT") if dt.sql == InternalGeometryType.sql => TypeCheckResult.TypeCheckSuccess
      case (dt, "HEX") if dt.sql == InternalGeometryType.sql => TypeCheckResult.TypeCheckSuccess
      case (dt, "GEOJSON") if dt.sql == InternalGeometryType.sql => TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(
          s"Cannot convert from ${inGeometry.dataType.sql} to $dataType"
        )
    }

  /** Expression output DataType. */
  override def dataType: DataType = outDataType.toUpperCase match {
    case "WKT" => StringType
    case "WKB" => BinaryType
    case "HEX" => HexType
    case "COORDS" => InternalGeometryType
    case "GEOJSON" => JSONType
  }

  override def toString: String = s"convert_to($inGeometry, $outDataType)"

  /** Overridden to ensure [[Expression.sql]] is properly formatted. */
  override def prettyName: String = "convert_to"

  /**
   * Depending on the input data type and output data type keyword a specific
   * conversion function will be executed after the case matching has been evaluated.
   * @param input Type-erased input data that will be casted to a correct
   *              data type based on the case matching and the called function.
   * @return  A converted representation of the input geometry.
   */
  override def nullSafeEval(input: Any): Any =
    (inGeometry.dataType, outDataType.toUpperCase) match {
      case (BinaryType, "WKT") => geom2wkt(wkb2geom(input))
      case (BinaryType, "HEX") => wkb2hex(input)
      case (BinaryType, "COORDS") => InternalGeometry(wkb2geom(input)).serialize
      case (BinaryType, "GEOJSON") => geom2geojson(wkb2geom(input))
      case (StringType, "WKB") => geom2wkb(wkt2geom(input))
      case (StringType, "HEX") => wkb2hex(geom2wkb(wkt2geom(input)))
      case (StringType, "COORDS") => InternalGeometry(wkt2geom(input)).serialize
      case (StringType, "GEOJSON") => geom2geojson(wkt2geom(input))
      case (HexType, "WKT") => geom2wkt(hex2geom(input))
      case (HexType, "WKB") => geom2wkb(hex2geom(input))
      case (HexType, "COORDS") => InternalGeometry(hex2geom(input)).serialize
      case (HexType, "GEOJSON") => geom2geojson(hex2geom(input))
      case (JSONType, "WKT") => geom2wkt(geojson2geom(input))
      case (JSONType, "WKB") => geom2wkb(geojson2geom(input))
      case (JSONType, "HEX") => geom2hex(geojson2geom(input))
      case (JSONType, "COORDS") => InternalGeometry(geojson2geom(input)).serialize
      case (InternalGeometryType, "WKB") => geom2wkb(InternalGeometry(input.asInstanceOf[InternalRow]).toGeom)
      case (InternalGeometryType, "WKT") => geom2wkt(InternalGeometry(input.asInstanceOf[InternalRow]).toGeom)
      case (InternalGeometryType, "HEX") => wkb2hex(geom2wkb(InternalGeometry(input.asInstanceOf[InternalRow]).toGeom))
      case (InternalGeometryType, "GEOJSON") => geom2geojson(InternalGeometry(input.asInstanceOf[InternalRow]).toGeom)
      case _ =>
        throw new Error(
          s"Cannot convert from ${inGeometry.dataType.sql} to $dataType"
        )
    }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val res = ConvertTo(
      newArgs(0).asInstanceOf[Expression],
      newArgs(1).asInstanceOf[String]
    )
    res.copyTagsFrom(this)
    res
  }

  override def child: Expression = inGeometry
}
