package com.databricks.labs.mosaic.datasource.multiread

import com.databricks.labs.mosaic.datasource.OGRFileFormat
import com.databricks.labs.mosaic.expressions.util.OGRReadeWithOffset
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import scala.collection.convert.ImplicitConversions.`dictionary AsScalaMap`
import scala.reflect.ClassTag

class OGRMultiReadDataFrameReader(sparkSession: SparkSession) extends MosaicDataFrameReader(sparkSession) {

    import sparkSession.implicits._

    override def load(path: String): DataFrame = load(Seq(path): _*)

    def load2(paths: String*): DataFrame = {
        val df = sparkSession.read
            .format("binaryFile")
            .load(paths: _*)
            .select("path")

        OGRFileFormat.enableOGRDrivers()
        val headPath = df.head().getString(0)

        val driverName = this.extraOptions.get("driverName").getOrElse("")
        val layerNumber = this.extraOptions.get("layerNumber").getOrElse("0").toInt
        val layerName = this.extraOptions.get("layerName").getOrElse("")
        val chunkSize = this.extraOptions.get("chunkSize").getOrElse("5000").toInt
        val vsizip = this.extraOptions.get("vsizip").getOrElse("false").toBoolean
        val inferenceLimit = this.extraOptions.get("inferenceLimit").getOrElse("100").toInt

        val ds = OGRFileFormat.getDataSource(driverName, headPath, vsizip)
        val layer = OGRFileFormat.getLayer(ds, layerNumber, layerName)
        val partitionCount = 1 + (layer.GetFeatureCount / chunkSize)

        val schema = OGRFileFormat.inferSchemaImpl(driverName, layerNumber, inferenceLimit, vsizip, headPath)

        val df2 = df
            .withColumn(
              "rowCount",
              lit(chunkSize)
            )
            .withColumn(
              "reader_n",
              explode(array((0 until partitionCount.toInt).map(lit(_)): _*))
            )
            .repartition(
              partitionCount.toInt,
              col("path"),
              col("reader_n")
            )
            .mapPartitions(parition =>
                parition.flatMap(row => {

                    val path = row.getString(0).replace("dbfs:/", "/dbfs/")
                    val rowCount = row.getInt(1)
                    val readerN = row.getInt(2)
                    OGRFileFormat.enableOGRDrivers()

                    val ds = OGRFileFormat.getDataSource(driverName, path, vsizip)
                    val layer = OGRFileFormat.getLayer(ds, layerNumber, layerName)
                    // layer.ResetReading()
                    val metadata = layer.GetMetadata_Dict().toMap

                    val start = readerN * rowCount
                    val end = math.min((readerN + 1) * rowCount, layer.GetFeatureCount())
                    layer.SetNextByIndex(start)
                    (start.toInt until end.toInt)
                        .map(_ => {

                            val feature = layer.GetNextFeature()
                            val fields = OGRFileFormat.getFeatureFields(feature, schema.get)

                            fields.map(_.toString).toSeq

                        })

                })
            )

        df2.toDF("fields_array")
            .select(
              schema.get.fields.zipWithIndex.map { case (f, i) => col("fields_array").getItem(i).cast(f.dataType).alias(f.name) }: _*
            )

    }

    override def load(paths: String*): DataFrame = {
        val df = sparkSession.read
            .format("binaryFile")
            .load(paths: _*)
            .select("path")

        OGRFileFormat.enableOGRDrivers()
        val headPath = df.head().getString(0)

        val driverName = this.extraOptions.get("driverName").getOrElse("")
        val layerNumber = this.extraOptions.get("layerNumber").getOrElse("0").toInt
        val layerName = this.extraOptions.get("layerName").getOrElse("")
        val chunkSize = this.extraOptions.get("chunkSize").getOrElse("5000").toInt
        val vsizip = this.extraOptions.get("vsizip").getOrElse("false").toBoolean
        val inferenceLimit = this.extraOptions.get("inferenceLimit").getOrElse("100").toInt

        val ds = OGRFileFormat.getDataSource(driverName, headPath, vsizip)
        val layer = OGRFileFormat.getLayer(ds, layerNumber, layerName)
        val partitionCount = 1 + (layer.GetFeatureCount / chunkSize)

        val schema = OGRFileFormat.inferSchemaImpl(driverName, layerNumber, inferenceLimit, vsizip, headPath)

        val ogrReadWithOffset = OGRReadeWithOffset(
          col("path").expr,
          col("chunkIndex").expr,
          Map(
            "driverName" -> driverName,
            "layerNumber" -> layerNumber.toString,
            "layerName" -> layerName,
            "chunkSize" -> chunkSize.toString,
            "vsizip" -> vsizip.toString,
            "inferenceLimit" -> inferenceLimit.toString
          ),
          schema.get
        )

        df
            .withColumn(
              "rowCount",
              lit(chunkSize)
            )
            .withColumn(
              "chunkIndex",
              explode(array((0 until partitionCount.toInt).map(lit(_)): _*))
            )
            .repartition(
              partitionCount.toInt,
              col("path"),
              col("reader_n")
            )
            .withColumn(
              "row_struct",
              org.apache.spark.sql.adapters.Column(ogrReadWithOffset)
            )
            .select(
              "row_struct.*"
            )

    }

}

object OGRMultiReadDataFrameReader {
    class ArrayAnyEncoder(runtimeSchema: StructType) extends Encoder[Array[Any]] {

        override def schema: StructType = runtimeSchema

        override def clsTag: ClassTag[Array[Any]] = ClassTag(classOf[Array[Any]])

    }
}
