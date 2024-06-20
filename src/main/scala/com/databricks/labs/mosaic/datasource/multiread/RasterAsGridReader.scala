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

    private var nPartitions = -1 // may change throughout the phases

    override def load(path: String): DataFrame = load(Seq(path): _*)

    override def load(paths: String*): DataFrame = {

        // scalastyle:off println

        // config
        // - turn off aqe coalesce partitions for this op
        sparkSession.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
        val config = getConfig

        nPartitions = config("nPartitions").toInt
        val resolution = config("resolution").toInt

        //println(
        //    s"raster_to_grid - nPartitions? $nPartitions | isRetile? ${config("retile").toBoolean} (tileSize? ${config("tileSize")}) ..."
        //)

        // (1) gdal reader load
        val pathsDf = sparkSession.read
            .format("gdal")
            .option("extensions", config("extensions"))
            .option(MOSAIC_RASTER_READ_STRATEGY, "as_path")
            .option("vsizip", config("vsizip"))
            .load(paths: _*)
            .repartition(nPartitions)

        // (2) increase nPartitions for retile and tessellate
        nPartitions = Math.min(10000, pathsDf.count() * 10).toInt
        //println(s"raster_to_grid - adjusted nPartitions to $nPartitions ...")

        // (3) combiner columnar function
        val rasterToGridCombiner = getRasterToGridFunc(config("combiner"))

        // (4) resolve subdataset
        // - writes resolved df to checkpoint dir
        val rasterDf = resolveRaster(pathsDf, config)

        // (5) retile with 'tileSize'
        val retiledDf = retileRaster(rasterDf, config)

        // (6) tessellate w/ combiner
        // - tessellate is checkpoint dir
        // - combiner is based on configured checkpointing
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

        // (7) handle k-ring resample
        kRingResample(loadedDf, config)

        // scalastyle:on println
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
        val isRetile = config.getOrElse("retile", "false").toBoolean
        val tileSize = config.getOrElse("tileSize", "-1").toInt

        if (isRetile && tileSize > 0) {
            // always uses the configured checkpoint path
            rasterDf
                .withColumn("tile", rst_retile(col("tile"), lit(tileSize), lit(tileSize)))
                .repartition(nPartitions)
        } else {
            rasterDf
        }
    }

    /**
      * Resolve the subdatasets if configured to do so. Resolving subdatasets
      * requires "subdatasetName" to be set to the desired subdataset to retrieve.
      *
      * @param pathsDf
      *   The DataFrame containing the paths.
      * @param config
      *   The configuration map.
      * @return
      *   The DataFrame containing the resolved subdatasets or the orginal paths
      *   if not configured to resolve subdatasets.
      */
    private def resolveRaster(pathsDf: DataFrame, config: Map[String, String]) = {
        val subdatasetName = config("subdatasetName")

        if (subdatasetName.nonEmpty) {
            pathsDf
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
        val k = config.getOrElse("kRingInterpolate", "0").toInt

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
            "vsizip" -> this.extraOptions.getOrElse("vsizip", "false"),
            "resolution" -> this.extraOptions.getOrElse("resolution", "0"),
            "combiner" -> this.extraOptions.getOrElse("combiner", "mean"),
            "kRingInterpolate" -> this.extraOptions.getOrElse("kRingInterpolate", "0"),
            "nPartitions" -> this.extraOptions.getOrElse("nPartitions", sparkSession.conf.get("spark.sql.shuffle.partitions")),
            "retile" -> this.extraOptions.getOrElse("retile", "true"),
            "tileSize" -> this.extraOptions.getOrElse("tileSize", "256"),
            "subdatasetName" -> this.extraOptions.getOrElse("subdatasetName", "")
        )
    }

}
