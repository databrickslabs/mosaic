package com.databricks.labs.mosaic.datasource.multiread

import com.databricks.labs.mosaic.datasource.OGRFileFormat
import com.databricks.labs.mosaic.expressions.util.OGRReadeWithOffset
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import scala.collection.convert.ImplicitConversions.`dictionary AsScalaMap`
import scala.reflect.ClassTag

class OGRMultiReadDataFrameReader(sparkSession: SparkSession) extends MosaicDataFrameReader(sparkSession) {

    private def getConfig: Map[String, String] = {
        Map(
            "driverName" -> this.extraOptions.get("driverName").getOrElse(""),
            "layerNumber" -> this.extraOptions.get("layerNumber").getOrElse("0"),
            "layerName" -> this.extraOptions.get("layerName").getOrElse(""),
            "chunkSize" -> this.extraOptions.get("chunkSize").getOrElse("5000"),
            "vsizip" -> this.extraOptions.get("vsizip").getOrElse("false"),
            "inferenceLimit" -> this.extraOptions.get("inferenceLimit").getOrElse("100")
        )
    }

    override def load(path: String): DataFrame = load(Seq(path): _*)

    override def load(paths: String*): DataFrame = {
        val df = sparkSession.read
            .format("binaryFile")
            .load(paths: _*)
            .select("path")

        OGRFileFormat.enableOGRDrivers()
        val headPath = df.head().getString(0)
        val config = getConfig

        val driverName = config("driverName")
        val layerNumber = config("layerNumber").toInt
        val layerName = config("layerName")
        val chunkSize = config("chunkSize").toInt
        val vsizip = config("vsizip").toBoolean
        val inferenceLimit = config("inferenceLimit").toInt

        val ds = OGRFileFormat.getDataSource(driverName, headPath, vsizip)
        val layer = OGRFileFormat.getLayer(ds, layerNumber, layerName)
        val partitionCount = 1 + (layer.GetFeatureCount / chunkSize)

        val schema = OGRFileFormat.inferSchemaImpl(driverName, layerNumber, inferenceLimit, vsizip, headPath)

        val ogrReadWithOffset = OGRReadeWithOffset(
          col("path").expr,
          col("chunkIndex").expr,
          config,
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
              col("chunkIndex")
            )
            .select(
                org.apache.spark.sql.adapters.Column(ogrReadWithOffset)
            )

    }

}
