package com.databricks.labs.mosaic.datasource.multiread

import com.databricks.labs.mosaic.{
    MOSAIC_RASTER_READ_AS_PATH,
    MOSAIC_RASTER_READ_STRATEGY,
    MOSAIC_RASTER_RE_TILE_ON_READ,
    NO_EXT
}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.utils.PathUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.util.Locale
import scala.util.Try

/*
 * A Mosaic DataFrame Reader that provides a unified interface to read GDAL tile data formats.
 * - It resolves the subdatasets if configured to read subdatasets.
 * - It then retiles the tile if configured to retile the tile.
 * - It converts the tile to a grid using the configured combiner.
 * - Finally, the grid is interpolated using the configured interpolation k ring size (if > 0).
 * The grid is then returned as a DataFrame.
 *
 * @param sparkSession
 *  The Spark Session to use for reading. This is required to create the DataFrame.
 */
class RasterAsGridReader(sparkSession: SparkSession) extends MosaicDataFrameReader(sparkSession) {
    // scalastyle:off println
    private val mc = MosaicContext.context()
    import mc.functions._

    private var nPartitions = -1 // may change throughout the phases

    override def load(path: String): DataFrame = load(Seq(path): _*)

    override def load(paths: String*): DataFrame = {

        // config
        // - turn off aqe coalesce partitions for this op
        sparkSession.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
        var config = getConfig

        nPartitions = config("nPartitions").toInt
        val resolution = config("resolution").toInt

        // NESTED HANDLING
        // "HDF4" -> "hdf4",
        // "HDF5" -> "hdf5",
        // "GRIB" -> "grb",
        // "netCDF" -> "nc",
        // "Zarr" -> "zarr"

        val nestedDrivers = Seq("hdf4", "hdf5", "grib", "netcdf", "zarr")
        val nestedExts = Seq("hdf4", "hdf5", "grb", "nc", "zarr")
        val driverName = config("driverName")

        val nestedHandling = {
            if (
                driverName.nonEmpty &&
                    nestedDrivers.contains(driverName.toLowerCase(Locale.ROOT))
            ) {
                println(s"... config 'driverName' identified for nestedHandling ('$driverName')")
                true
            } else if (
                config("extensions").split(";").map(p => p.trim.toLowerCase(Locale.ROOT))
                    .exists(nestedExts.contains)
            ) {
                println(s"... config 'extensions' identified for nestedHandling ('${config("extensions")}')")
                true
            } else if (
                paths.map(p => PathUtils.getExtOptFromPath(p, None).getOrElse(NO_EXT).toLowerCase(Locale.ROOT))
                    .exists(nestedExts.contains)
                ) {
                println(s"... path ext identified for nestedHandling")
                true
            } else {
                false
            }
        }
        // update "sizeInMB" if missing for nestedHandling
        // - want pretty small splits for dense data
        if (nestedHandling && config("sizeInMB").toInt < 1) {
            config = getConfig + (
                "sizeInMB" -> "8",
                "retile" -> "false",
                "tileSize" -> "-1"
            )
        }
        val readStrat = {
            // have to go out of way to specify "-1"
            if (config("sizeInMB").toInt < 0) MOSAIC_RASTER_READ_AS_PATH
            else MOSAIC_RASTER_RE_TILE_ON_READ
        }

        println(
            s"raster_to_grid - nestedHandling? $nestedHandling | nPartitions? $nPartitions | read strat? $readStrat ..."
        )
        println(s"config (after any mods) -> $config")

        val baseOptions = Map(
            "extensions" -> config("extensions"),
            "vsizip" -> config("vsizip"),
            "subdatasetName" -> config("subdatasetName"),
            MOSAIC_RASTER_READ_STRATEGY -> readStrat
        )
        val readOptions =
            if (driverName.nonEmpty && config("sizeInMB").toInt >= 1) {
                baseOptions +
                    ("driverName" -> driverName, "sizeInMB" -> config("sizeInMB"))
            }
            else if (driverName.nonEmpty) baseOptions + ("driverName" -> driverName)
            else if (config("sizeInMB").toInt >= 1) baseOptions + ("sizeInMB" -> config("sizeInMB"))
            else baseOptions
        println(s"raster_to_grid - readOptions? $readOptions ...")

        val rasterToGridCombiner = getRasterToGridFunc(config("combiner"))
        var pathsDf: DataFrame = null
        var rasterDf: DataFrame = null
        var retiledDf: DataFrame = null
        var tessellatedDf: DataFrame = null
        var combinedDf: DataFrame = null
        var bandDf: DataFrame = null
        var kSampleDf: DataFrame = null

        try {
            // (1) gdal reader load
            pathsDf = sparkSession.read
                .format("gdal")
                .options(readOptions)
                .load(paths: _*)
                .repartition(nPartitions)
                .cache()
            val pathsDfCnt = pathsDf.count()
            println(s"::: (1) gdal reader loaded - count? $pathsDfCnt :::")

            // (2) increase nPartitions for retile and tessellate
            nPartitions = Math.min(10000, paths.length * 32).toInt
            println(s"::: (2) adjusted nPartitions to $nPartitions :::")

            // (3) resolve subdataset
            // - writes resolved df to checkpoint dir
            rasterDf = resolveRaster(pathsDf, config).cache()
            val rasterDfCnt = rasterDf.count()
            pathsDf.unpersist() // <- let go of prior caching
            println(s"::: (3) resolved subdataset - count? $rasterDfCnt :::")

            // (4) retile with 'tileSize'
            retiledDf = retileRaster(rasterDf, config).cache()
            val retiledDfCnt = retiledDf.count()
            println(s"::: (4) retiled with 'tileSize' - count? $retiledDfCnt :::")

            // (5) tessellation
            // - uses checkpoint dir
            tessellatedDf = retiledDf
                .withColumn(
                    "tile",
                    rst_tessellate(col("tile"), lit(0))
                ).cache()
            var tessellatedDfCnt = tessellatedDf.count()
            retiledDf.unpersist() // <- let go of prior caching
            println(s"... tessellated at resolution 0 - count? $tessellatedDfCnt (going to $resolution)")

            if (resolution > 0) {
                for (res <- 1 to resolution) {
                    tessellatedDf = tessellatedDf
                        .withColumn(
                            s"tile_$res",
                            rst_tessellate(col("tile"), lit(res))
                        )
                        .drop("tile")
                        .filter(col(s"tile_$res").isNotNull)
                        .withColumnRenamed(s"tile_$res", "tile")
                        .cache()
                    tessellatedDfCnt = tessellatedDf.count()
                    println(s"... tessellated at resolution $res - count? $tessellatedDfCnt (going to $resolution)")
                }
            }
            println(s"::: (5) tessellated :::")

            // (6) combine
            // - uses checkpoint dir
            combinedDf = tessellatedDf
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
                .cache()
            val combinedDfCnt = combinedDf.count()
            println(s"::: (6) combined - count? $combinedDfCnt :::")

            // (7) band exploded
            bandDf = combinedDf
                    .select(
                        posexplode(col("grid_measures")).as(Seq("band_id", "measure")),
                        col("tile").getField("index_id").alias("cell_id")
                    )
                    .select(
                        col("band_id"),
                        col("cell_id"),
                        col("measure")
                    ).cache()
            val bandDfCnt = bandDf.count()
            println(s"::: (7) band exploded - count? $bandDfCnt :::")

            // (8) handle k-ring resample
            // - returns cached
            kSampleDf = kRingResample(bandDf, config).cache()
            val kSampleDfCnt = kSampleDf.count()
            println(s"::: (8) k-ring resampled - count? $kSampleDfCnt :::")

            kSampleDf
        } finally {
            Try(pathsDf.unpersist())
            Try(rasterDf.unpersist())
            Try(retiledDf.unpersist())
            Try(tessellatedDf.unpersist())
            Try(combinedDf.unpersist())
            Try(bandDf.unpersist())
        }
    }

    /**
      * Retile the tile if configured to do so. Retiling requires "retile" to
      * be set to true in the configuration map. It also requires "tileSize" to
      * be set to the desired tile size.
      * @param rasterDf
      *   The DataFrame containing the rasters.
      * @param config
      *   The configuration map.
      * @return
      *   The tile to grid function.
      */
    private def retileRaster(rasterDf: DataFrame, config: Map[String, String]) = {
        val isRetile = config.getOrElse("retile", "false").toBoolean
        val tileSize = config.getOrElse("tileSize", "-1").toInt

        if (isRetile && tileSize > 0) {
            println(s"... retiling to tileSize = $tileSize")
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
            println(s"... resolving subdatasetName = $subdatasetName")
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
            println(s"... kRingInterpolate = $k rings")
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
      * Get the tile to grid function based on the combiner.
      * @param combiner
      *   The combiner to use.
      * @return
      *   The tile to grid function.
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
            "retile" -> this.extraOptions.getOrElse("retile", "false"),
            "sizeInMB" -> this.extraOptions.getOrElse("sizeInMB", "0"),
            "tileSize" -> this.extraOptions.getOrElse("tileSize", "512"),
            "subdatasetName" -> this.extraOptions.getOrElse("subdatasetName", ""),
            "driverName" -> this.extraOptions.getOrElse("driverName", ""),
            "uriDeepCheck" -> this.extraOptions.getOrElse("uriDeepCheck", "false")
        )
    }
    // scalastyle:on println

}
