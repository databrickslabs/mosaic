package com.databricks.labs.mosaic.datasource

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
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
        val driverName = options.getOrElse("driverName", "")
        inferSchemaImpl(driverName, layerN, files)
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

        buildReaderImpl(driverName, layerN)
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

    def tryParse(value: String): DataType = {
        Seq(
          (Try(value.toInt).toOption, IntegerType),
          (Try(value.toDouble).toOption, DoubleType),
          (Try(value.toBoolean).toOption, BooleanType),
          (Try(value.toLong).toOption, LongType),
          (Try(value.toFloat).toOption, FloatType),
          (Try(value.toShort).toOption, ShortType),
          (Try(value.toByte).toOption, ByteType),
          (Try(DateTimeUtils.stringToDate(UTF8String.fromString(value))).toOption, DateType),
          (Try(DateTimeUtils.stringToTimestampWithoutTimeZone(UTF8String.fromString(value))).toOption, TimestampType)
        ).find(_._1.isDefined).map(_._2).getOrElse(StringType)
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
    def getValue(feature: Feature, j: Int): Any = {
        val field = feature.GetFieldDefnRef(j)
        val typeName = field.GetFieldTypeName(j)
        val dataType = getType(typeName)
        dataType match {
            case IntegerType               => feature.GetFieldAsInteger(j)
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
      * OGR feature.
      * @param id
      * field index.
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
      * OGR feature.
      * @param id
      * field index.
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
        import com.databricks.labs.mosaic.expressions.raster
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
                val typeName = field.GetFieldTypeName(j)
                val dataType = StructField(f"field_${j}_$name", getType(typeName))
                val tryDataType = StructField(f"field_${j}_$name", tryParse(feature.GetFieldAsString(j)))
                coerceTypes(dataType, tryDataType)
            }) ++ (0 until feature.GetGeomFieldCount())
            .map(j => {
                val field = feature.GetGeomFieldDefnRef(j)
                val name = field.GetNameRef
                StructField(f"geom_${j}_$name", BinaryType, nullable = true)
            })
        StructType(fields)
    }

    def coerceTypes(s: StructField, f: StructField): StructField = {
        val ordered = Seq(s, f).sortBy(_.dataType.prettyJson)
        val first = ordered.head
        val second = ordered.last
        (first.dataType, second.dataType) match {
            case (DoubleType, FloatType)              => StructField(s.name, DoubleType, s.nullable)
            case (DoubleType, IntegerType)            => StructField(s.name, DoubleType, s.nullable)
            case (DoubleType, LongType)               => StructField(s.name, DoubleType, s.nullable)
            case (FloatType, IntegerType)             => StructField(s.name, FloatType, s.nullable)
            case (FloatType, LongType)                => StructField(s.name, FloatType, s.nullable)
            case (IntegerType, LongType)              => StructField(s.name, LongType, s.nullable)
            case (DateType, IntegerType)              => StructField(s.name, DateType, s.nullable)
            case (DateType, LongType)                 => StructField(s.name, TimestampType, s.nullable)
            case (DateType, TimestampType)            => StructField(s.name, TimestampType, s.nullable)
            case (TimestampType, LongType)            => StructField(s.name, TimestampType, s.nullable)
            case (TimestampType, IntegerType)         => StructField(s.name, TimestampType, s.nullable)
            case (ArrayType(t1, _), ArrayType(t2, _)) => StructField(
                  s.name,
                  ArrayType(coerceTypes(StructField("", t1), StructField("", t2)).dataType, containsNull = true),
                  s.nullable
                )
            case (_, StringType)                      => StructField(s.name, StringType, s.nullable)
            case _                                    => StructField(s.name, StringType, s.nullable)
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
        files: Seq[FileStatus]
    ): Option[StructType] = {

        val path = files.head.getPath.toString.replace("file:", "")
        val dataset =
            if (driverName.isEmpty) {
                ogr.Open(path)
            } else {
                ogr.GetDriverByName(driverName).Open(path)
            }

        val layer = dataset.GetLayer(layerN)
        val headFeature = layer.GetNextFeature()
        val headSchemaFields = getFeatureSchema(headFeature).fields

        // start from 1 since 1 feature was read already
        val layerSchema = (1 until layer.GetFeatureCount()).foldLeft(headSchemaFields) { (schema, _) =>
            val feature = layer.GetNextFeature()
            val featureSchema = getFeatureSchema(feature)
            schema.zip(featureSchema.fields).map { case (s, f) => coerceTypes(s, f) }
        }

        Some(StructType(layerSchema))

    }

    def buildReaderImpl(
        driverName: String,
        layerN: Int
    ): PartitionedFile => Iterator[InternalRow] = { file: PartitionedFile =>
        {
            val path = file.filePath.replace("file:", "")
            val dataset =
                if (driverName.isEmpty) {
                    ogr.Open(path)
                } else {
                    ogr.GetDriverByName(driverName).Open(path)
                }

            val metadata = dataset.GetMetadata_Dict().toMap
            val layer = dataset.GetLayerByIndex(layerN)

            var feature = layer.GetNextFeature()
            (0 until layer.GetFeatureCount().toInt)
                .foldLeft(Seq.empty[InternalRow])((acc, _) => {
                    val fields = (0 until feature.GetFieldCount())
                        .map(j => getValue(feature, j))
                    val geoms = (0 until feature.GetGeomFieldCount())
                        .map(feature.GetGeomFieldRef(_).ExportToWkb())
                    val values = fields ++ geoms ++ Seq(metadata)
                    val row = createRow(values)
                    feature = layer.GetNextFeature()
                    acc ++ Seq(row)
                })
                .iterator
        }
    }

}
