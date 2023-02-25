package com.databricks.labs.mosaic.datasource

import com.databricks.labs.mosaic.expressions.raster
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.ogr.{ogr, Feature}

import scala.collection.convert.ImplicitConversions.`dictionary AsScalaMap`
import scala.util.Try

/**
  * A base Spark SQL data source for reading OGR data sources. It reads from a
  * single layer of a data source. The layer number is specified by the option
  * "layerNumber". The data source driver name is specified by the option
  * "driverName".
  */
class OGRFileFormat extends FileFormat with DataSourceRegister {

    import com.databricks.labs.mosaic.datasource.OGRFileFormat._

    override def shortName(): String = "ogr"

    override def inferSchema(
        sparkSession: SparkSession,
        options: Map[String, String],
        files: Seq[FileStatus]
    ): Option[StructType] = {
        val layerN = options.getOrElse("layerNumber", "0").toInt
        val inferenceLimit = options.getOrElse("inferenceLimit", "200").toInt
        val driverName = options.getOrElse("driverName", "")
        val useZipPath = options.getOrElse("vsizip", "false").toBoolean
        inferSchemaImpl(driverName, layerN, inferenceLimit, useZipPath, files)
    }

    override def isSplitable(
        sparkSession: SparkSession,
        options: Map[String, String],
        path: org.apache.hadoop.fs.Path
    ): Boolean = false

    override def buildReader(
        sparkSession: SparkSession,
        dataSchema: StructType,
        partitionSchema: StructType,
        requiredSchema: StructType,
        filters: Seq[Filter],
        options: Map[String, String],
        hadoopConf: Configuration
    ): PartitionedFile => Iterator[InternalRow] = {
        val layerN = options.getOrElse("layerNumber", "0").toInt
        val driverName = options.getOrElse("driverName", "")
        val useZipPath = options.getOrElse("vsizip", "false").toBoolean
        buildReaderImpl(driverName, layerN, useZipPath, dataSchema)
    }

    override def prepareWrite(
        sparkSession: SparkSession,
        job: Job,
        options: Map[String, String],
        dataSchema: StructType
    ): OutputWriterFactory = throw new Error("Not implemented")

}

//noinspection VarCouldBeVal
object OGRFileFormat {

    /**
      * Converts a OGR type name to Spark SQL data type.
      *
      * @param typeName
      *   the OGR type name.
      * @return
      *   the Spark SQL data type.
      */
    def getType(typeName: String): DataType =
        typeName match {
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
      * Infers the type of a field from a feature. The type is inferred from the
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
    def inferType(feature: Feature, j: Int): DataType = {
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

            val inferredType =
                if (coerceables.isEmpty) StringType
                else if (coerceables.contains(LongType)) LongType
                else if (coerceables.contains(DoubleType)) DoubleType
                else if (coerceables.contains(FloatType)) FloatType
                else if (coerceables.contains(IntegerType)) IntegerType
                else if (coerceables.contains(ShortType)) ShortType
                else if (coerceables.contains(ByteType)) ByteType
                else if (coerceables.contains(BooleanType)) BooleanType
                else if (coerceables.contains(DateType)) DateType
                else if (coerceables.contains(TimestampType)) TimestampType
                else StringType

            if (reportedType == StringType && inferredType != StringType) inferredType
            else TypeCoercion.findTightestCommonType(reportedType, inferredType).getOrElse(StringType)
        }
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
      * Extracts the value of a date field from a feature.
      *
      * @param feature
      *   OGR feature.
      * @param id
      *   field index.
      * @return
      */
    // noinspection ScalaDeprecation
    def getDate(feature: Feature, id: Int): Int = {
        var year: Array[Int] = Array.fill[Int](1)(0)
        var month: Array[Int] = Array.fill[Int](1)(0)
        var day: Array[Int] = Array.fill[Int](1)(0)
        var hour: Array[Int] = Array.fill[Int](1)(0)
        var minute: Array[Int] = Array.fill[Int](1)(0)
        var second: Array[Float] = Array.fill[Float](1)(0)
        var tz: Array[Int] = Array.fill[Int](1)(0)
        feature.GetFieldAsDateTime(id, year, month, day, hour, minute, second, tz)
        val date = new java.sql.Date(year(0), month(0), day(0))
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
    // noinspection ScalaDeprecation
    def getDateTime(feature: Feature, id: Int): Long = {
        var year: Array[Int] = Array.fill[Int](1)(0)
        var month: Array[Int] = Array.fill[Int](1)(0)
        var day: Array[Int] = Array.fill[Int](1)(0)
        var hour: Array[Int] = Array.fill[Int](1)(0)
        var minute: Array[Int] = Array.fill[Int](1)(0)
        var second: Array[Float] = Array.fill[Float](1)(0)
        var tz: Array[Int] = Array.fill[Int](1)(0)
        feature.GetFieldAsDateTime(id, year, month, day, hour, minute, second, tz)
        val datetime = new java.sql.Timestamp(year(0), month(0), day(0), hour(0), minute(0), second(0).toInt, tz(0))
        DateTimeUtils.fromJavaTimestamp(datetime)
    }

    /**
      * Creates a Spark SQL row from a sequence of values.
      *
      * @param values
      *   sequence of values.
      * @return
      *   Spark SQL row.
      */
    def createRow(values: Seq[Any]): InternalRow = {
        InternalRow.fromSeq(
          values.map {
              case null           => null
              case b: Array[Byte] => b
              case v: Array[_]    => new GenericArrayData(v)
              case m: Map[_, _]   => raster.buildMapString(m.map { case (k, v) => (k.toString, v.toString) })
              case s: String      => UTF8String.fromString(s)
              case v              => v
          }
        )
    }

    /**
      * Creates a Spark SQL schema from a OGR feature.
      *
      * @param feature
      *   OGR feature.
      * @return
      *   Spark SQL schema.
      */
    def getFeatureSchema(feature: Feature): StructType = {
        val fields = (0 until feature.GetFieldCount())
            .map(j => {
                val field = feature.GetFieldDefnRef(j)
                val name = field.GetNameRef
                val fieldName = if (name.isEmpty) f"field_$j" else name
                StructField(fieldName, inferType(feature, j))
            }) ++ (0 until feature.GetGeomFieldCount())
            .map(j => {
                val field = feature.GetGeomFieldDefnRef(j)
                val name = field.GetNameRef
                val geomName = if (name.isEmpty) f"geom_$j" else name
                StructField(geomName, BinaryType, nullable = true)
            })
        StructType(fields)
    }

    /**
      * Load the data source from the given path using the specified driver.
      *
      * @param driverName
      *   the name of the OGR driver
      * @param path
      *   the path to the file
      * @return
      *   the data source
      */
    def getDataSource(driverName: String, path: String, useZipPath: Boolean): org.gdal.ogr.DataSource = {
        val cleanPath = path.replace("file:/", "/").replace("dbfs:/", "/dbfs/")
        // 0 is for no update driver
        if (useZipPath && cleanPath.endsWith(".zip") && driverName.nonEmpty) {
            ogr.GetDriverByName(driverName).Open(s"/vsizip/$cleanPath", 0)
        } else if (useZipPath && cleanPath.endsWith(".zip")) {
            ogr.Open(s"/vsizip/$cleanPath", 0)
        } else if (driverName.nonEmpty) {
            ogr.GetDriverByName(driverName).Open(cleanPath, 0)
        } else {
            ogr.Open(cleanPath, 0)
        }
    }

    /**
      * Infer the schema of a OGR file.
      *
      * @param driverName
      *   the name of the OGR driver
      * @param layerN
      *   the layer number for which to infer the schema
      * @param files
      *   the files to infer the schema from
      * @return
      *   the inferred schema for the given files and layer
      */
    def inferSchemaImpl(
        driverName: String,
        layerN: Int,
        inferenceLimit: Int,
        useZipPath: Boolean,
        files: Seq[FileStatus]
    ): Option[StructType] = {

        val path = files.head.getPath.toString
        val dataset = getDataSource(driverName, path, useZipPath)

        val layer = dataset.GetLayer(layerN)
        layer.ResetReading()
        val headFeature = layer.GetNextFeature()
        val headSchemaFields = getFeatureSchema(headFeature).fields
        val n = math.min(inferenceLimit, layer.GetFeatureCount()).toInt

        // start from 1 since 1 feature was read already
        val layerSchema = (1 until n).foldLeft(headSchemaFields) { (schema, _) =>
            val feature = layer.GetNextFeature()
            val featureSchema = getFeatureSchema(feature)
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

    def buildReaderImpl(
        driverName: String,
        layerN: Int,
        useZipPath: Boolean,
        schema: StructType
    ): PartitionedFile => Iterator[InternalRow] = { file: PartitionedFile =>
        {
            val path = file.filePath
            val dataset = getDataSource(driverName, path, useZipPath)

            val metadata = dataset.GetMetadata_Dict().toMap
            val layer = dataset.GetLayerByIndex(layerN)
            layer.ResetReading()
            val types = schema.fields.map(_.dataType)

            var feature: Feature = null
            (0 until layer.GetFeatureCount().toInt)
                .foldLeft(Seq.empty[InternalRow])((acc, _) => {
                    feature = layer.GetNextFeature()
                    val fields = (0 until feature.GetFieldCount())
                        .map(j => getValue(feature, j, types(j)))
                    val geoms = (0 until feature.GetGeomFieldCount())
                        .map(feature.GetGeomFieldRef(_).ExportToWkb())
                    val values = fields ++ geoms ++ Seq(metadata)
                    val row = createRow(values)
                    acc ++ Seq(row)
                })
                .iterator
        }
    }

}
