package com.databricks.labs.mosaic.datasource

import com.databricks.labs.mosaic.utils.PathUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.ogr._

import java.sql.Timestamp
import scala.collection.convert.ImplicitConversions.`dictionary AsScalaMap`
import scala.util.Try

/**
  * A base Spark SQL data source for reading OGR data sources. It reads from a
  * single layer of a data source. The layer number is specified by the option
  * "layerNumber". The data source driver name is specified by the option
  * "driverName".
  */
class OGRFileFormat extends FileFormat with DataSourceRegister with Serializable {

    import com.databricks.labs.mosaic.datasource.OGRFileFormat._

    override def shortName(): String = "ogr"

    override def inferSchema(
        sparkSession: SparkSession,
        options: Map[String, String],
        files: Seq[FileStatus]
    ): Option[StructType] = {
        val driverName = options.getOrElse("driverName", "")
        val headFilePath = files.head.getPath.toString
        inferSchemaImpl(driverName, headFilePath, options)
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

        val driverName = options.getOrElse("driverName", "")
        // No column filter at the moment.
        // To improve performance, we can filter columns in the OGR layer using requiredSchema.
        buildReaderImpl(driverName, dataSchema, requiredSchema, options)
    }

    override def prepareWrite(
        sparkSession: SparkSession,
        job: Job,
        options: Map[String, String],
        dataSchema: StructType
    ): OutputWriterFactory = throw new Error("Not implemented")

}

//noinspection VarCouldBeVal
object OGRFileFormat extends Serializable {

    def OGREmptyGeometry: Geometry = {
        enableOGRDrivers()
        ogr.CreateGeometryFromWkt("POINT EMPTY")
    }

    /**
      * Get the layer from a data source. The method prioritizes the layer name
      * over the layer number.
      *
      * @param ds
      *   the data source.
      * @param layerNumber
      *   the layer number.
      * @param layerName
      *   the layer name.
      * @return
      *   the layer.
      */
    def getLayer(ds: org.gdal.ogr.DataSource, layerNumber: Int, layerName: String): Layer = {
        if (layerName.isEmpty) {
            ds.GetLayer(layerNumber)
        } else {
            ds.GetLayerByName(layerName)
        }
    }

    /** Registers all OGR drivers if they haven't been registered yet. */
    final def enableOGRDrivers(force: Boolean = false): Unit = {
        val drivers = ogr.GetDriverCount
        if (drivers == 0 || force) {
            ogr.RegisterAll()
        }
    }

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
    def coerceTypeList(coerceables: Seq[DataType]): DataType = {
        if (coerceables.isEmpty) StringType
        else if (coerceables.contains(LongType)) LongType
        else if (coerceables.contains(DoubleType)) DoubleType
        else if (coerceables.contains(FloatType)) FloatType
        else if (coerceables.contains(IntegerType)) IntegerType
        else if (coerceables.contains(ShortType)) ShortType
        else if (coerceables.contains(BinaryType)) BinaryType
        else if (coerceables.contains(ByteType)) ByteType
        else if (coerceables.contains(BooleanType)) BooleanType
        else if (coerceables.contains(DateType)) DateType
        else if (coerceables.contains(TimestampType)) TimestampType
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
    def getJavaSQLTimestamp(feature: Feature, id: Int): Timestamp = {
        var year: Array[Int] = Array.fill[Int](1)(0)
        var month: Array[Int] = Array.fill[Int](1)(0)
        var day: Array[Int] = Array.fill[Int](1)(0)
        var hour: Array[Int] = Array.fill[Int](1)(0)
        var minute: Array[Int] = Array.fill[Int](1)(0)
        var second: Array[Float] = Array.fill[Float](1)(0)
        var tz: Array[Int] = Array.fill[Int](1)(0)
        feature.GetFieldAsDateTime(id, year, month, day, hour, minute, second, tz)
        val datetime = new java.sql.Timestamp(year(0), month(0), day(0), hour(0), minute(0), second(0).toInt, tz(0))
        datetime
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
        val timestamp = getJavaSQLTimestamp(feature, id)
        val date = new java.sql.Date(timestamp.getYear, timestamp.getMonth, timestamp.getDay)
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
    def getDateTime(feature: Feature, id: Int): Long = {
        val datetime = getJavaSQLTimestamp(feature, id)
        DateTimeUtils.fromJavaTimestamp(datetime)
    }

    /**
      * Creates a Spark SQL schema from a OGR feature.
      *
      * @param feature
      *   OGR feature.
      * @return
      *   Spark SQL schema.
      */
    def getFeatureSchema(feature: Feature, asWKB: Boolean): StructType = {
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
                  StructField(geomName + "_srid", StringType)
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
                    //f.FlattenTo2D()
                    Seq(
                      if (asWKB) f.ExportToWkb else f.ExportToWkt,
                      Try(f.GetSpatialReference.GetAuthorityCode(null)).getOrElse("0")
                    )
                } else {
                    Seq(
                      if (asWKB) OGREmptyGeometry.ExportToWkb else OGREmptyGeometry.ExportToWkt,
                      "0"
                    )
                }

            })
        val values = fields ++ geoms
        values.toArray
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
    def getDataSource(driverName: String, path: String): org.gdal.ogr.DataSource = {
        val cleanPath = PathUtils.getCleanPath(path)
        // 0 is for no update driver
        if (driverName.nonEmpty) {
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
        OGRFileFormat.enableOGRDrivers()

        val layerN = options.getOrElse("layerNumber", "0").toInt
        val layerName = options.getOrElse("layerName", "")
        val inferenceLimit = options.getOrElse("inferenceLimit", "200").toInt
        val asWKB = options.getOrElse("asWKB", "false").toBoolean

        val dataset = getDataSource(driverName, path)
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

    /**
      * Build a reader for a OGR file.
      *
      * @param driverName
      *   the name of the OGR driver
      * @param dataSchema
      *   the full schema of the file
      * @param requiredSchema
      *   the schema of the file that is required for the query
      * @param options
      *   the options to use for the reader
      * @return
      *   a function that can be used to read the file
      */
    def buildReaderImpl(
        driverName: String,
        dataSchema: StructType,
        requiredSchema: StructType,
        options: Map[String, String]
    ): PartitionedFile => Iterator[InternalRow] = { file: PartitionedFile =>
        {
            OGRFileFormat.enableOGRDrivers()

            val layerN = options.getOrElse("layerNumber", "0").toInt
            val layerName = options.getOrElse("layerName", "")
            val asWKB = options.getOrElse("asWKB", "false").toBoolean
            val path = file.filePath
            val dataset = getDataSource(driverName, path.toString())
            val resolvedLayerName = if (layerName.isEmpty) dataset.GetLayer(layerN).GetName() else layerName
            val layer = dataset.GetLayerByName(resolvedLayerName)
            layer.ResetReading()
            val metadata = layer.GetMetadata_Dict().toMap
            val mask = dataSchema.map(_.name).map(requiredSchema.fieldNames.contains(_)).toArray

            var feature: Feature = null
            (0 until layer.GetFeatureCount().toInt)
                .foldLeft(Seq.empty[InternalRow])((acc, _) => {
                    feature = layer.GetNextFeature()
                    val fields = getFeatureFields(feature, dataSchema, asWKB)
                        .zip(mask)
                        .filter(_._2)
                        .map(_._1)
                    val values = fields ++ Seq(metadata)
                    val row = Utils.createRow(values)
                    acc ++ Seq(row)
                })
                .iterator
        }
    }

}
