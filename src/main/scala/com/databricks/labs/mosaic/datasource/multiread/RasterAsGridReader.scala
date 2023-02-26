package com.databricks.labs.mosaic.datasource.multiread

import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class RasterAsGridReader(sparkSession: SparkSession) extends MosaicDataFrameReader(sparkSession) {

    private val mc = MosaicContext.context()
    mc.getRasterAPI.enable()
    import mc.functions._

    override def load(path: String): DataFrame = load(Seq(path): _*)

    override def load(paths: String*): DataFrame = {

        val config = getConfig
        val resolution = config("resolution").toInt

        val pathsDf = sparkSession.read
            .format("binaryFile")
            .load(paths: _*)
            .select("path")

        val rasterToGridCombiner = getRasterToGridFunc(config("combiner"))

        val rasterDf = resolveRaster(pathsDf, config)

        val retiledDf = retileRaster(rasterDf, config)

        retiledDf
            .withColumn(
              "grid_measures",
              rasterToGridCombiner(col("raster"), lit(resolution))
            )
            .select(
              "grid_measures",
              "raster"
            )
            .select(
              posexplode(col("grid_measures")).as(Seq("band_id", "grid_measures")),
              col("raster")
            )
            .select(
              explode(col("grid_measures")).alias("grid_measures"),
              col("subdataset")
            )
            .select(
              col("grid_measures").getItem("cellID").alias("cell_id"),
              col("grid_measures").getItem("measure").alias("measure")
            )
            .groupBy("cell_id")
            .agg(avg("measure").alias("measure"))

    }

    private def retileRaster(rasterDf: DataFrame, config: Map[String, String]) = {
        val retile = config("retile").toBoolean
        val tileSize = config("tileSize").toInt

        if (retile) {
            rasterDf.withColumn(
              "raster",
              rst_retile(col("raster"), lit(tileSize), lit(tileSize))
            )
        } else {
            rasterDf
        }
    }

    private def resolveRaster(pathsDf: DataFrame, config: Map[String, String]) = {
        val readSubdataset = config("readSubdataset").toBoolean
        val subdatasetNumber = config("subdatasetNumber").toInt
        val subdatasetName = config("subdatasetName")

        if (readSubdataset) {
            pathsDf
                .withColumn(
                  "subdatasets",
                  rst_subdatasets("path")
                )
                .withColumn(
                  "subdataset",
                  if (subdatasetName.isEmpty) {
                      element_at(map_keys(col("subdatasets")), subdatasetNumber)
                  } else {
                      element_at(col("subdatasets"), subdatasetName)
                  }
                )
                .select(
                  col("subdataset").alias("raster")
                )
        } else {
            pathsDf.select(
              col("path").alias("raster")
            )
        }
    }

    private def getRasterToGridFunc(combiner: String): (Column, Column) => Column = {
        combiner match {
            case "mean"    => rst_rastertogridavg
            case "min"     => rst_rastertogridmin
            case "max"     => rst_rastertogridmax
            case "median"  => rst_rastertogridmedian
            case "count"   => rst_rastertogridcount
            case "average" => rst_rastertogridavg
            case "avg"     => rst_rastertogridavg
            case _         => throw new Error("Combiner not supported")
        }
    }

    private def getConfig: Map[String, String] = {
        Map(
          "readSubdataset" -> this.extraOptions.getOrElse("retile", "false"),
          "subdatasetNumber" -> this.extraOptions.getOrElse("subdatasetNumber", "0"),
          "subdatasetName" -> this.extraOptions.getOrElse("subdatasetName", ""),
          "resolution" -> this.extraOptions.getOrElse("resolution", "0"),
          "combiner" -> this.extraOptions.getOrElse("combiner", "mean"),
          "retile" -> this.extraOptions.getOrElse("retile", "false"),
          "tileSize" -> this.extraOptions.getOrElse("tileSize", "256")
        )
    }

}
