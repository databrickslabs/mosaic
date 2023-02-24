package com.databricks.labs.mosaic.datasource

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.ogr.{ogr, Feature}

import scala.collection.convert.ImplicitConversions.`dictionary AsScalaMap`

/**
  * A base Spark SQL data source for reading OGR data sources. It reads from a
  * single layer of a data source. The layer number is specified by the option
  * "layerNumber". The data source driver name is specified by the option
  * "driverName".
  */
abstract class OGRFileFormat extends FileFormat
      with DataSourceRegister {

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
      *   OGR feature.
      * @param id
      *   field index.
      * @return
      */
    // noinspection ScalaDeprecation
    def getDate(feature: Feature, id: Int): java.sql.Date = {
        var year: Array[Int] = Array.fill[Int](1)(0)
        var month: Array[Int] = Array.fill[Int](1)(0)
        var day: Array[Int] = Array.fill[Int](1)(0)
        feature.GetFieldAsDateTime(id, year, month, day, null, null, null, null)
        new java.sql.Date(year(0), month(0), day(0))
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
    def getDateTime(feature: Feature, id: Int): java.sql.Timestamp = {
        var year: Array[Int] = Array.fill[Int](1)(0)
        var month: Array[Int] = Array.fill[Int](1)(0)
        var day: Array[Int] = Array.fill[Int](1)(0)
        var hour: Array[Int] = Array.fill[Int](1)(0)
        var minute: Array[Int] = Array.fill[Int](1)(0)
        var second: Array[Float] = Array.fill[Float](1)(0)
        var tz: Array[Int] = Array.fill[Int](1)(0)
        feature.GetFieldAsDateTime(id, year, month, day, hour, minute, second, tz)
        new java.sql.Timestamp(year(0), month(0), day(0), hour(0), minute(0), second(0).toInt, tz(0))
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
              case v: Array[_]    => new GenericArrayData(v)
              case m: Map[_, _]   => raster.buildMapString(m.map { case (k, v) => (k.toString, v.toString) })
              case s: String      => UTF8String.fromString(s)
              case b: Array[Byte] => b
              case v              => v
          }
        )
    }

    /**
      * Infer the schema of a OGR file.
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
        val headFeature = layer.GetFeature(0)

        val layerSchema = (0 until headFeature.GetFieldCount())
            .map(j => {
                val field = headFeature.GetFieldDefnRef(j)
                val name = field.GetNameRef
                val typeName = field.GetFieldTypeName(j)
                val dataType = getType(typeName)
                StructField(f"field_${j}_$name", dataType, nullable = true)
            }) ++ (0 until headFeature.GetGeomFieldCount())
            .map(j => {
                val field = headFeature.GetGeomFieldDefnRef(j)
                val name = field.GetNameRef
                StructField(f"geom_${j}_$name", BinaryType, nullable = true)
            })

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

            (0 until layer.GetFeatureCount().toInt)
                .map(i => {
                    val feature = layer.GetFeature(i)
                    val fields = (0 until feature.GetFieldCount())
                        .map(j => getValue(feature, j))
                    val geoms = (0 until feature.GetGeomFieldCount())
                        .map(feature.GetGeomFieldRef(_).ExportToWkb())
                    val values = fields ++ geoms ++ Seq(metadata)
                    val row = createRow(values)
                    row
                })
                .iterator
        }
    }

}
