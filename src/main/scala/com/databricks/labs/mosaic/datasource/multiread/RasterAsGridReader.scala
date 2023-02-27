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

        val loadedDf = retiledDf
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
              col("raster")
            )
            .select(
              col("grid_measures").getItem("cellID").alias("cell_id"),
              col("grid_measures").getItem("measure").alias("measure")
            )
            .groupBy("cell_id")
            .agg(avg("measure").alias("measure"))

        kRingResample(loadedDf, config)

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
                  rst_subdatasets(col("path"))
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

    private def kRingResample(rasterDf: DataFrame, config: Map[String, String]) = {
        val k = config("kRingInterpolate").toInt

        def weighted_sum(measureCol: String, weightCol: String) = {
            sum(col(measureCol) * col(weightCol)) / sum(col(weightCol))
        }.alias(measureCol)

        if (k > 0) {
            rasterDf
                .withColumn("origin_cell_id", col("cell_id"))
                .withColumn("cell_id", explode(grid_cellkring(col("origin_cell_id"), k)))
                .withColumn("weight", lit(k + 1) - expr("h3_distance(origin_cell_id, cell_id)"))
                .groupBy("cell_id")
                .agg(weighted_sum("measure", "weight"))
        } else {
            rasterDf
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
          "readSubdataset" -> this.extraOptions.getOrElse("readSubdataset", "false"),
          "subdatasetNumber" -> this.extraOptions.getOrElse("subdatasetNumber", "0"),
          "subdatasetName" -> this.extraOptions.getOrElse("subdatasetName", ""),
          "resolution" -> this.extraOptions.getOrElse("resolution", "0"),
          "combiner" -> this.extraOptions.getOrElse("combiner", "mean"),
          "retile" -> this.extraOptions.getOrElse("retile", "false"),
          "tileSize" -> this.extraOptions.getOrElse("tileSize", "256"),
          "kRingInterpolate" -> this.extraOptions.getOrElse("kRingInterpolate", "0")
        )
    }

}
