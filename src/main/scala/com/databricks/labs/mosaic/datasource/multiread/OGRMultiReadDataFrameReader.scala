package com.databricks.labs.mosaic.datasource.multiread

import com.databricks.labs.mosaic.datasource.OGRFileFormat
import com.databricks.labs.mosaic.expressions.util.OGRReadeWithOffset
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
  * A Mosaic DataFrame Reader that provides a unified interface to read OGR
  * formats. It uses the binaryFile reader to list the file paths and then uses
  * the OGRFileFormat to read the data. It reads the first file in the list to
  * determine the schema and then uses that schema to read the rest of the
  * files. All the files in the path must have the same schema. It reads the
  * files with offsets to avoid reading the entire file. This increases the
  * level of parallelism of the reading. The chunk size can be set with the
  * chunkSize option. The default is 5000.
  * @param sparkSession
  *   The Spark Session to use for reading. This is required to create the
  *   DataFrame.
  */
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

    /**
      * Get the configuration for the OGR reader.
      * @return
      *   A map of the configuration.
      */
    private def getConfig: Map[String, String] = {
        Map(
          "driverName" -> this.extraOptions.getOrElse("driverName", ""),
          "layerNumber" -> this.extraOptions.getOrElse("layerNumber", "0"),
          "layerName" -> this.extraOptions.getOrElse("layerName", ""),
          "chunkSize" -> this.extraOptions.getOrElse("chunkSize", "5000"),
          "vsizip" -> this.extraOptions.getOrElse("vsizip", "false"),
          "asWKB" -> this.extraOptions.getOrElse("asWKB", "false")
        )
    }

}
