package com.databricks.labs.gbx.vectorx.ds.ogr

import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.ogr._

import java.sql.Timestamp
import scala.util.Try

//noinspection VarCouldBeVal
object OGR_SchemaInference extends Serializable {

    private def OGREmptyGeometry: Geometry = {
        enableOGRDrivers()
        ogr.CreateGeometryFromWkt("POINT EMPTY")
    }

    /** Registers all OGR drivers if they haven't been registered yet. */
    final def enableOGRDrivers(force: Boolean = false): Unit = {
        val drivers = ogr.GetDriverCount
        if (drivers == 0 || force) {
            ogr.RegisterAll()
        }
    }

    /**
      * Converts an OGR type name to Spark SQL data type.
      *
      * @param typeName
      *   the OGR type name.
      * @return
      *   the Spark SQL data type.
      */
    def getType(typeName: String): DataType =
        typeName match {
            case "Boolean"        => BooleanType
            case "Integer"        => IntegerType
            case "String"         => StringType
            case "Real"           => DoubleType
            case "Date"           => DateType
            case "Time"           => TimestampType
            case "DateTime"       => TimestampType
            case "Binary"         => BinaryType
            case "IntegerList"    => ArrayType(IntegerType)
            case "RealList"       => ArrayType(DoubleType)
            case "StringList"     => ArrayType(StringType)
            case "WideString"     => StringType
            case "WideStringList" => ArrayType(StringType)
            case "Integer64"      => LongType
            case _                => StringType
        }

    /**
      * Infers the type of field from a feature. The type is inferred from the
      * value of the field. If the field type is String, the inferred type is
      * returned. Otherwise, the inferred type is coerced to the reported type.
      * If the reported type is not String, the inferred type is coerced to the
      * reported type.
      * @param feature
      *   the feature
      * @param j
      *   the field index
      * @return
      *   the inferred type
      */
    private def inferType(feature: Feature, j: Int): DataType = {
        val field = feature.GetFieldDefnRef(j)
        val reportedType = getType(field.GetFieldTypeName(field.GetFieldType))
        val value = feature.GetFieldAsString(j)

        if (value == "") {
            reportedType
        } else {
            val coerceables = Seq(
              (Try(value.toInt).toOption, IntegerType),
              (Try(value.toDouble).toOption, DoubleType),
              (Try(value.toBoolean).toOption, BooleanType),
              (Try(value.toLong).toOption, LongType),
              (Try(value.toFloat).toOption, FloatType),
              (Try(value.toShort).toOption, ShortType),
              (Try(value.toByte).toOption, ByteType),
              (Try(DateTimeUtils.stringToDate(UTF8String.fromString(value)).get).toOption, DateType),
              (Try(DateTimeUtils.stringToTimestampWithoutTimeZone(UTF8String.fromString(value)).get).toOption, TimestampType)
            ).filter(_._1.isDefined).map(_._2)

            val inferredType = coerceTypeList(coerceables)
            if (reportedType == StringType && inferredType != StringType) inferredType
            else TypeCoercion.findTightestCommonType(reportedType, inferredType).getOrElse(StringType)
        }
    }

    /**
      * Coerces a list of types to a single type.
      *
      * @param coerceables
      *   the list of types.
      * @return
      *   the coerced type.
      */
    private[ds] def coerceTypeList(coerceables: Seq[DataType]): DataType = {
        if (coerceables.isEmpty) StringType
        else if (coerceables.contains(StringType)) StringType
        else if (coerceables.contains(LongType)) LongType
        else if (coerceables.contains(DoubleType)) DoubleType
        else if (coerceables.contains(FloatType)) FloatType
        else if (coerceables.contains(IntegerType)) IntegerType
        else if (coerceables.contains(ShortType)) ShortType
        else if (coerceables.contains(BinaryType)) BinaryType
        else if (coerceables.contains(ByteType)) ByteType
        else if (coerceables.contains(BooleanType)) BooleanType
        else if (coerceables.contains(TimestampType)) TimestampType
        else if (coerceables.contains(DateType)) DateType
        else StringType
    }

    /**
      * Extracts the value of a field from a feature. The type of the value is
      * determined by the field type.
      *
      * @param feature
      *   OGR feature.
      * @param j
      *   field index.
      * @return
      */
    def getValue(feature: Feature, j: Int, dataType: DataType): Any = {
        dataType match {
            case IntegerType               => feature.GetFieldAsInteger(j)
            case LongType                  => feature.GetFieldAsInteger64(j)
            case StringType                => feature.GetFieldAsString(j)
            case DoubleType                => feature.GetFieldAsDouble(j)
            case DateType                  => getDate(feature, j)
            case TimestampType             => getDateTime(feature, j)
            case BinaryType                => feature.GetFieldAsBinary(j)
            case ArrayType(IntegerType, _) => feature.GetFieldAsIntegerList(j)
            case ArrayType(DoubleType, _)  => feature.GetFieldAsDoubleList(j)
            case ArrayType(StringType, _)  => feature.GetFieldAsStringList(j)
            case _                         => feature.GetFieldAsString(j)
        }
    }

    /**
      * Return the field index of a field name if it exists.
      *
      * @param feature
      *   the OGR feature.
      * @param name
      *   the field name.
      * @return
      *   the field index.
      */
    def getFieldIndex(feature: Feature, name: String): Option[Int] = {
        val field = feature.GetFieldDefnRef(name)
        if (field == null) None
        else (0 until feature.GetFieldCount).find(i => feature.GetFieldDefnRef(i).GetName == name)
    }

    /**
      * Converts a OGR date to a java.sql.Date.
      *
      * @param feature
      *   the OGR feature.
      * @param id
      *   the field index.
      * @return
      *   the java.sql.Date.
      */
    // noinspection ScalaDeprecation
    private def getJavaSQLTimestamp(feature: Feature, id: Int): Timestamp = {
        var year: Array[Int] = Array.fill[Int](1)(0)
        var month: Array[Int] = Array.fill[Int](1)(0)
        var day: Array[Int] = Array.fill[Int](1)(0)
        var hour: Array[Int] = Array.fill[Int](1)(0)
        var minute: Array[Int] = Array.fill[Int](1)(0)
        var second: Array[Float] = Array.fill[Float](1)(0)
        var tz: Array[Int] = Array.fill[Int](1)(0)
        feature.GetFieldAsDateTime(id, year, month, day, hour, minute, second, tz)

        val y = year(0)
        val m = month(0)
        val d = day(0)
        val H = hour(0)
        val M = minute(0)
        val s = second(0)

        val sInt = math.floor(s).toInt
        val nanos = ((s - sInt) * 1e9).round.toInt.max(0)

        val ldt = java.time.LocalDateTime.of(y, m, d, H, M, sInt, nanos)

        val inst = tz(0) match {
            case 100   => ldt.toInstant(java.time.ZoneOffset.UTC) // UTC
            case 0 | 1 => ldt.atZone(java.time.ZoneId.systemDefault()).toInstant // unknown/local
            case off   => ldt.atOffset(java.time.ZoneOffset.ofTotalSeconds(off * 60)).toInstant // minutes offset
        }

        val ts: java.sql.Timestamp = java.sql.Timestamp.from(inst)
        ts
    }

    /**
      * Extracts the value of a date field from a feature.
      *
      * @param feature
      *   OGR feature.
      * @param id
      *   field index.
      * @return
      */
    private def getDate(feature: Feature, id: Int): Int = {
        val timestamp = getJavaSQLTimestamp(feature, id)
        val localDate = timestamp.toLocalDateTime.toLocalDate       // correct Y-M-D
        val date = java.sql.Date.valueOf(localDate)                 // build sql.Date properly
        DateTimeUtils.fromJavaDate(date)
    }

    /**
      * Extracts the value of a date-time field from a feature.
      *
      * @param feature
      *   OGR feature.
      * @param id
      *   field index.
      * @return
      */
    private def getDateTime(feature: Feature, id: Int): Long = {
        val datetime = getJavaSQLTimestamp(feature, id)
        DateTimeUtils.fromJavaTimestamp(datetime)
    }

    /**
      * Creates a Spark SQL schema from an OGR feature.
      *
      * @param feature
      *   OGR feature.
      * @return
      *   Spark SQL schema.
      */
    private def getFeatureSchema(feature: Feature, asWKB: Boolean): StructType = {
        val geomDataType = if (asWKB) BinaryType else StringType
        val fields = (0 until feature.GetFieldCount())
            .map(j => {
                val field = feature.GetFieldDefnRef(j)
                val name = field.GetNameRef
                val fieldName = if (name.isEmpty) f"field_$j" else name
                StructField(fieldName, inferType(feature, j))
            }) ++ (0 until feature.GetGeomFieldCount())
            .flatMap(j => {
                val field = feature.GetGeomFieldDefnRef(j)
                val name = field.GetNameRef
                val geomName = if (name.isEmpty) f"geom_$j" else name
                Seq(
                  StructField(geomName, geomDataType),
                  StructField(geomName + "_srid", StringType),
                  StructField(geomName + "_srid_proj", StringType)
                )
            })
        StructType(fields)
    }

    /**
      * Get the fields of a feature as an array of values.
      * @param feature
      *   OGR feature.
      * @param featureSchema
      *   Spark SQL schema.
      * @return
      *   Array of values.
      */
    def getFeatureFields(feature: Feature, featureSchema: StructType, asWKB: Boolean): Array[Any] = {
        val types = featureSchema.fields.map(_.dataType)
        val fields = (0 until feature.GetFieldCount())
            .map(j => getValue(feature, j, types(j)))
        val geoms = (0 until feature.GetGeomFieldCount())
            .map(feature.GetGeomFieldRef)
            .flatMap(f => {
                if (Option(f).isDefined) {
                    // f.FlattenTo2D()
                    Seq(
                      if (asWKB) f.ExportToWkb else f.ExportToWkt,
                      Try(f.GetSpatialReference.GetAuthorityCode(null)).getOrElse("0"),
                      Try(f.GetSpatialReference.ExportToProj4).getOrElse("")
                    )
                } else {
                    Seq(
                      if (asWKB) OGREmptyGeometry.ExportToWkb else OGREmptyGeometry.ExportToWkt,
                      "0",
                      ""
                    )
                }

            })
        val values = fields ++ geoms
        values.toArray
    }

    /**
      * Infer the schema of an OGR file.
      *
      * @param driverName
      *   the name of the OGR driver
      * @param path
      *   the path to the file to infer the schema from
      * @param options
      *   the options to use for the inference
      * @return
      *   the inferred schema for the given files and layer
      */
    def inferSchemaImpl(
        driverName: String,
        path: String,
        options: Map[String, String]
    ): Option[StructType] = {
        ogr.RegisterAll()
        val layerN = options.getOrElse("layerNumber", "0").toInt
        val layerName = options.getOrElse("layerName", "")
        val inferenceLimit = options.getOrElse("inferenceLimit", "100").toInt
        val asWKB = options.getOrElse("asWKB", "true").toBoolean

        val dataset = OGR_Driver.open(path, driverName)
        val resolvedLayerName = if (layerName.isEmpty) dataset.GetLayer(layerN).GetName() else layerName
        val layer = dataset.GetLayer(resolvedLayerName)
        layer.ResetReading()
        val headFeature = layer.GetNextFeature()
        val headSchemaFields = getFeatureSchema(headFeature, asWKB).fields
        val n = math.min(inferenceLimit, layer.GetFeatureCount()).toInt

        // start from 1 since 1 feature was read already
        val layerSchema = (1 until n).foldLeft(headSchemaFields) { (schema, _) =>
            val feature = layer.GetNextFeature()
            val featureSchema = getFeatureSchema(feature, asWKB)
            schema.zip(featureSchema.fields).map { case (s, f) =>
                (s, f) match {
                    case (StructField(name, StringType, _, _), StructField(_, dataType, _, _)) =>
                        StructField(name, dataType, nullable = true)
                    case (StructField(name, dataType, _, _), StructField(_, StringType, _, _)) =>
                        StructField(name, dataType, nullable = true)
                    case (StructField(name, dataType, _, _), StructField(_, dataType2, _, _))  =>
                        StructField(name, TypeCoercion.findTightestCommonType(dataType2, dataType).getOrElse(StringType), nullable = true)
                }
            }
        }

        Some(StructType(layerSchema))

    }

}
