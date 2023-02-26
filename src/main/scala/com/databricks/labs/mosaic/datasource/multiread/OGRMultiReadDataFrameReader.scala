package com.databricks.labs.mosaic.datasource.multiread

import com.databricks.labs.mosaic.datasource.OGRFileFormat
import com.databricks.labs.mosaic.expressions.util.OGRReadeWithOffset
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class OGRMultiReadDataFrameReader(sparkSession: SparkSession) extends MosaicDataFrameReader(sparkSession) {

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

        val ds = OGRFileFormat.getDataSource(driverName, headPath, vsizip)
        val layer = OGRFileFormat.getLayer(ds, layerNumber, layerName)
        val partitionCount = 1 + (layer.GetFeatureCount / chunkSize)

        val schema = OGRFileFormat.inferSchemaImpl(driverName, headPath, config)

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

    private def getConfig: Map[String, String] = {
        Map(
          "driverName" -> this.extraOptions.getOrElse("driverName", ""),
          "layerNumber" -> this.extraOptions.getOrElse("layerNumber", "0"),
          "layerName" -> this.extraOptions.getOrElse("layerName", ""),
          "chunkSize" -> this.extraOptions.getOrElse("chunkSize", "5000"),
          "vsizip" -> this.extraOptions.getOrElse("vsizip", "false"),
          "inferenceLimit" -> this.extraOptions.getOrElse("inferenceLimit", "100"),
          "asWKB" -> this.extraOptions.getOrElse("asWKB", "false")
        )
    }

}
