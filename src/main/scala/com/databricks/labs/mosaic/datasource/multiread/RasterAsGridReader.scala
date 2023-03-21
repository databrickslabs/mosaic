package com.databricks.labs.mosaic.datasource.multiread

import com.databricks.labs.mosaic.datasource.GDALFileFormat
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/*
 * A Mosaic DataFrame Reader that provides a unified interface to read GDAL raster data
 * formats. It uses the binaryFile reader to list the raster files. It then resolves the
 * subdatasets if configured to read subdatasets. It then retiles the raster if configured
 * to retile the raster. It converts the raster to a grid using the configured
 * combiner. The grid is then returned as a DataFrame. Finally, the grid is interpolated
 * using the configured interpolation k ring size.
 * @param sparkSession
 *  The Spark Session to use for reading. This is required to create the DataFrame.
 */
class RasterAsGridReader(sparkSession: SparkSession) extends MosaicDataFrameReader(sparkSession) {

    private val mc = MosaicContext.context()
    mc.getRasterAPI.enable()
    import mc.functions._

    val vsizipPathColF: Column => Column = (path: Column) =>
        when(
            path.endsWith(".zip"),
            concat(lit("/vsizip/"), path)
        ).otherwise(path)

    override def load(path: String): DataFrame = load(Seq(path): _*)

    override def load(paths: String*): DataFrame = {

        val config = getConfig
        val resolution = config("resolution").toInt

        val pathsDf = sparkSession.read
            .option("pathGlobFilter", config("fileExtension"))
            .format("binaryFile")
            .load(paths: _*)
            .select("path")
            .select(
              vsizipPathColF(col("path")).alias("path")
            )

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
              col("raster"),
              col("band_id"),
              explode(col("grid_measures")).alias("grid_measures")
            )
            .select(
              col("band_id"),
              col("grid_measures").getItem("cellID").alias("cell_id"),
              col("grid_measures").getItem("measure").alias("measure")
            )
            .groupBy("band_id", "cell_id")
            .agg(avg("measure").alias("measure"))

        kRingResample(loadedDf, config)

    }

    /**
      * Retile the raster if configured to do so. Retiling requires "retile" to
      * be set to true in the configuration map. It also requires "tileSize" to
      * be set to the desired tile size.
      * @param rasterDf
      *   The DataFrame containing the rasters.
      * @param config
      *   The configuration map.
      * @return
      *   The raster to grid function.
      */
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

    /**
      * Resolve the subdatasets if configured to do so. Resolving subdatasets
      * requires "readSubdataset" to be set to true in the configuration map. It
      * also requires "subdatasetNumber" to be set to the desired subdataset
      * number. If "subdatasetName" is set, it will be used instead of
      * "subdatasetNumber".
      * @param pathsDf
      *   The DataFrame containing the paths.
      * @param config
      *   The configuration map.
      * @return
      *   The DataFrame containing the resolved subdatasets or the orginal paths
      *   if not configured to resolve subdatasets.
      */
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
                    vsizipPathColF(col("subdataset")).alias("raster")
                )
        } else {
            pathsDf.select(
              col("path").alias("raster")
            )
        }
    }

    /**
      * Interpolate the grid using the k ring size if configured to do so.
      * Interpolation requires "kRingInterpolate" to be set to the desired k
      * ring size in the configuration map. If "kRingInterpolate" is set to a
      * value greater than 0, the grid will be interpolated using the k ring
      * size. Otherwise, the grid will be returned as is. The interpolation is
      * done using the inverse distance weighted sum of the k ring cells.
      * @param rasterDf
      *   The DataFrame containing the grid.
      * @param config
      *   The configuration map.
      * @return
      *   The DataFrame containing the interpolated grid.
      */
    private def kRingResample(rasterDf: DataFrame, config: Map[String, String]) = {
        val k = config("kRingInterpolate").toInt

        def weighted_sum(measureCol: String, weightCol: String) = {
            sum(col(measureCol) * col(weightCol)) / sum(col(weightCol))
        }.alias(measureCol)

        if (k > 0) {
            rasterDf
                .withColumn("origin_cell_id", col("cell_id"))
                .withColumn("cell_id", explode(grid_cellkring(col("origin_cell_id"), k)))
                .withColumn("weight", lit(k + 1) - grid_distance(col("origin_cell_id"), col("cell_id")))
                .groupBy("band_id", "cell_id")
                .agg(weighted_sum("measure", "weight"))
        } else {
            rasterDf
        }
    }

    /**
      * Get the raster to grid function based on the combiner.
      * @param combiner
      *   The combiner to use.
      * @return
      *   The raster to grid function.
      */
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

    /**
      * Get the configuration map.
      * @return
      *   The configuration map.
      */
    private def getConfig: Map[String, String] = {
        Map(
          "fileExtension" -> this.extraOptions.getOrElse("fileExtension", "*"),
          "readSubdataset" -> this.extraOptions.getOrElse("readSubdataset", "false"),
          "vsizip" -> this.extraOptions.getOrElse("vsizip", "false"),
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
