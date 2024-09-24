package com.databricks.labs.mosaic.datasource.multiread

import com.databricks.labs.mosaic.MOSAIC_RASTER_READ_STRATEGY
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
    import mc.functions._

    def getNPartitions(config: Map[String, String]): Int = {
        val shufflePartitions = sparkSession.conf.get("spark.sql.shuffle.partitions")
        val nPartitions = config.getOrElse("nPartitions", shufflePartitions).toInt
        nPartitions
    }

    private def workerNCores = {
        sparkSession.sparkContext.range(0, 1).map(_ => java.lang.Runtime.getRuntime.availableProcessors).collect.head
    }

    private def nWorkers = sparkSession.sparkContext.getExecutorMemoryStatus.size

    override def load(path: String): DataFrame = load(Seq(path): _*)

    override def load(paths: String*): DataFrame = {

        val config = getConfig
        val resolution = config("resolution").toInt
        val nPartitions = getNPartitions(config)
        val readStrategy = config("retile") match {
            case "true" => "retile_on_read"
            case _      => "in_memory"
        }
        val tileSize = config("sizeInMB").toInt

        val nCores = nWorkers * workerNCores
        val stageCoefficient = math.ceil(math.log(nCores) / math.log(4))

        val firstStageSize = (tileSize * math.pow(4, stageCoefficient)).toInt

        val pathsDf = sparkSession.read
            .format("gdal")
            .option("extensions", config("extensions"))
            .option(MOSAIC_RASTER_READ_STRATEGY, readStrategy)
            .option("vsizip", config("vsizip"))
            .option("sizeInMB", firstStageSize)
            .load(paths: _*)
            .repartition(nPartitions)

        val rasterToGridCombiner = getRasterToGridFunc(config("combiner"))

        val rasterDf = resolveRaster(pathsDf, config)

        val retiledDf = retileRaster(rasterDf, config)

        val loadedDf = retiledDf
            .withColumn(
              "tile",
              rst_tessellate(col("tile"), lit(resolution))
            )
            .repartition(nPartitions)
            .groupBy("tile.index_id")
            .agg(rst_combineavg_agg(col("tile")).alias("tile"))
            .withColumn(
              "grid_measures",
              rasterToGridCombiner(col("tile"))
            )
            .select(
              "grid_measures",
              "tile"
            )
            .select(
              posexplode(col("grid_measures")).as(Seq("band_id", "measure")),
              col("tile").getField("index_id").alias("cell_id")
            )
            .repartition(nPartitions)
            .select(
              col("band_id"),
              col("cell_id"),
              col("measure")
            )

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
        val tileSize = config.getOrElse("tileSize", "-1").toInt
        val memSize = config.getOrElse("sizeInMB", "-1").toInt
        val nPartitions = getNPartitions(config)

        if (retile) {
            if (memSize > 0) {
                rasterDf
                    .withColumn("tile", rst_subdivide(col("tile"), lit(memSize)))
                    .repartition(nPartitions)
            } else if (tileSize > 0) {
                rasterDf
                    .withColumn("tile", rst_retile(col("tile"), lit(tileSize), lit(tileSize)))
                    .repartition(nPartitions)
            } else {
                rasterDf
            }
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
        val subdatasetName = config("subdatasetName")

        if (readSubdataset) {
            pathsDf
                .withColumn("subdatasets", rst_subdatasets(col("tile")))
                .withColumn("tile", rst_getsubdataset(col("tile"), lit(subdatasetName)))
        } else {
            pathsDf.select(col("tile"))
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
        val nPartitions = getNPartitions(config)

        def weighted_sum(measureCol: String, weightCol: String) = {
            sum(col(measureCol) * col(weightCol)) / sum(col(weightCol))
        }.alias(measureCol)

        if (k > 0) {
            rasterDf
                .withColumn("origin_cell_id", col("cell_id"))
                .withColumn("cell_id", explode(grid_cellkring(col("origin_cell_id"), k)))
                .repartition(nPartitions)
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
    private def getRasterToGridFunc(combiner: String): Column => Column = {
        combiner match {
            case "mean"    => rst_avg
            case "min"     => rst_min
            case "max"     => rst_max
            case "median"  => rst_median
            case "count"   => rst_pixelcount
            case "average" => rst_avg
            case "avg"     => rst_avg
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
          "extensions" -> this.extraOptions.getOrElse("extensions", "*"),
          "readSubdataset" -> this.extraOptions.getOrElse("readSubdataset", "false"),
          "vsizip" -> this.extraOptions.getOrElse("vsizip", "false"),
          "subdatasetNumber" -> this.extraOptions.getOrElse("subdatasetNumber", "0"),
          "subdatasetName" -> this.extraOptions.getOrElse("subdatasetName", ""),
          "resolution" -> this.extraOptions.getOrElse("resolution", "0"),
          "combiner" -> this.extraOptions.getOrElse("combiner", "mean"),
          "retile" -> this.extraOptions.getOrElse("retile", "false"),
          "tileSize" -> this.extraOptions.getOrElse("tileSize", "-1"),
          "sizeInMB" -> this.extraOptions.getOrElse("sizeInMB", "-1"),
          "kRingInterpolate" -> this.extraOptions.getOrElse("kRingInterpolate", "0")
        )
    }

}
