package com.databricks.labs.mosaic.datasource

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.gdal.ogr.{ogr, Feature}

import scala.collection.convert.ImplicitConversions.`dictionary AsScalaMap`

class ShapefileFileFormat extends FileFormat with DataSourceRegister {

    import ShapefileFileFormat._

    override def inferSchema(
        sparkSession: SparkSession,
        options: Map[String, String],
        files: Seq[FileStatus]
    ): Option[StructType] = {

        val layerN = options.getOrElse("layerN", "0").toInt

        val path = files.head.getPath
        val dataset = ogr.GetDriverByName("ESRI Shapefile").Open(path.toString, 0)

        val layer = dataset.GetLayer(layerN)
        val headFeature = layer.GetFeature(0)

        val layerSchema = (0 until headFeature.GetFieldCount())
            .map(j => {
                val field = headFeature.GetFieldDefnRef(j)
                val name = field.GetNameRef
                val typeName = field.GetFieldTypeName(j)
                val dataType = getType(typeName)
                StructField(f"layer_${j}_$name", dataType, nullable = true)
            }) ++ (0 until headFeature.GetGeomFieldCount())
            .map(j => {
                val field = headFeature.GetGeomFieldDefnRef(j)
                val name = field.GetNameRef
                StructField(f"layer_${j}_$name", BinaryType, nullable = true)
            })

        Some(StructType(layerSchema))

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

        val layerN = options.getOrElse("layerN", "0").toInt

        file: PartitionedFile => {
            val path = file.filePath
            val dataset = ogr.GetDriverByName("ESRI Shapefile").Open(path, 0)

            val metadata = dataset.GetMetadata_Dict().toMap
            val layer = dataset.GetLayerByIndex(layerN)

            (0 until layer.GetFeatureCount())
                .map(i => {
                    val feature = layer.GetFeature(i)
                    val fields = (0 until feature.GetFieldCount())
                        .map(j => getValue(feature, j))
                    val geoms = (0 until feature.GetGeomFieldCount())
                        .map(feature.GetGeomFieldRef(_).ExportToWkb())
                    val values = fields ++ geoms ++ Seq(metadata)
                    InternalRow.fromSeq(values)
                })
                .iterator
        }
    }

    override def prepareWrite(
        sparkSession: SparkSession,
        job: Job,
        options: Map[String, String],
        dataSchema: StructType
    ): OutputWriterFactory = ???

    override def shortName(): String = "shapefile"

}

//noinspection VarCouldBeVal
object ShapefileFileFormat {

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

    // noinspection ScalaDeprecation
    def getDate(feature: Feature, id: Int): java.sql.Date = {
        var year: Array[Int] = Array.fill[Int](1)(0)
        var month: Array[Int] = Array.fill[Int](1)(0)
        var day: Array[Int] = Array.fill[Int](1)(0)
        feature.GetFieldAsDateTime(id, year, month, day, null, null, null, null)
        new java.sql.Date(year(0), month(0), day(0))
    }

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

}
