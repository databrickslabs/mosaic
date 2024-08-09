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

        println("\n<<< raster_to_grid invoked >>>")

        // <<< CONFIG >>>
        // - turn off aqe coalesce partitions for this op
        var config = getConfig
        nPartitions = config("nPartitions").toInt
        val resolution = config("resolution").toInt
        val verboseLevel = config("verboseLevel").toInt

        sparkSession.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
        if (verboseLevel > 0) println(s"raster_to_grid -> 'spark.sql.adaptive.coalescePartitions.enabled' set to false")

        // <<< NESTED HANDLING >>>
        val nestedDrivers = Seq("hdf4", "hdf5", "grib", "netcdf", "zarr")
        val nestedExts = Seq("hdf4", "hdf5", "grb", "nc", "zarr")
        val driverName = config("driverName")

        val nestedHandling = {
            if (config("vsizip").toBoolean) {
                false // <- skip subdivide for zips
            } else if (
                driverName.nonEmpty &&
                    nestedDrivers.contains(driverName.toLowerCase(Locale.ROOT))
            ) {
                 if (verboseLevel > 1) println(s"raster_to_grid -> config 'driverName' identified for nestedHandling ('$driverName')")
                true
            } else if (
                config("extensions").split(";").map(p => p.trim.toLowerCase(Locale.ROOT))
                    .exists(nestedExts.contains)
            ) {
                if (verboseLevel > 1)  println(s"raster_to_grid -> config 'extensions' identified for nestedHandling ('${config("extensions")}')")
                true
            } else if (
                paths.map(p => PathUtils.getExtOptFromPath(p, None).getOrElse(NO_EXT).toLowerCase(Locale.ROOT))
                    .exists(p => nestedExts.contains(p.toLowerCase(Locale.ROOT)))
                ) {
                if (verboseLevel > 1) println(s"raster_to_grid -> path ext identified for nestedHandling")
                true
            } else {
                false
            }
        }
        if (nestedHandling) {
            // nested handling
            // - update "sizeInMB" if missing,
            //   want pretty small splits for dense data
            // - update "retile" to false / "tileSize" to -1
            if (config("sizeInMB").toInt != 0) {
                config = getConfig + (
                    "retile" -> "false",
                    "tileSize" -> "-1"
                )
            } else {
                config = getConfig + (
                    "sizeInMB" -> "8",
                    "retile" -> "false",
                    "tileSize" -> "-1"
                )
            }
        } else if (!nestedHandling && config("vsizip").toBoolean) {
            // vsizip handling
            // - update "sizeInMB" to -1
            // - update "retile" to false / "tileSize" to -1
            config = getConfig + (
                "sizeInMB" -> "-1",
                "retile" -> "false",
                "tileSize" -> "-1"
            )
        }

        // <<< GDAL READER OPTIONS >>>
        val readStrat = {
            // have to go out of way to specify "-1"
            // don't use subdivide strategy with zips (AKA MOSAIC_RASTER_RE_TILE_ON_READ)
            if (config("sizeInMB").toInt < 0 || config("vsizip").toBoolean) MOSAIC_RASTER_READ_AS_PATH
            else MOSAIC_RASTER_RE_TILE_ON_READ
        }

        if (verboseLevel > 0) println(
            s"raster_to_grid -> nestedHandling? $nestedHandling | nPartitions? $nPartitions | read strat? $readStrat"
        )
        if (verboseLevel > 1) println(s"\nraster_to_grid - config (after any mods)? $config\n")

        val baseOptions = Map(
            "extensions" -> config("extensions"),
            "vsizip" -> config("vsizip"),
            "subdatasetName" -> config("subdatasetName"),
            MOSAIC_RASTER_READ_STRATEGY -> readStrat
        )
        val readOptions =
            if (driverName.nonEmpty && readStrat == MOSAIC_RASTER_RE_TILE_ON_READ) {
                baseOptions +
                    ("driverName" -> driverName, "sizeInMB" -> config("sizeInMB"))
            }
            else if (driverName.nonEmpty) baseOptions + ("driverName" -> driverName)
            else if (readStrat == MOSAIC_RASTER_RE_TILE_ON_READ) baseOptions + ("sizeInMB" -> config("sizeInMB"))
            else baseOptions
        if (verboseLevel > 1) println(s"\nraster_to_grid - readOptions? $readOptions\n")

        // <<< PERFORM READ >>>
        val rasterToGridCombiner = getRasterToGridFunc(config("combiner"))
        var pathsDf: DataFrame = null
        var resolvedDf: DataFrame = null
        var sridDf: DataFrame = null
        var retiledDf: DataFrame = null
        var tessellatedDf: DataFrame = null
        var combinedDf: DataFrame = null
        var bandDf: DataFrame = null
        var validDf: DataFrame = null
        var invalidDf: DataFrame = null
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
            println(s"::: gdal reader loaded - count? $pathsDfCnt :::")
            if (verboseLevel > 1) pathsDf.limit(1).show()

            // (2) resolve subdataset (if directed)
            // - metadata cache handled in the function
            resolvedDf = resolveSubdataset(pathsDf, config, verboseLevel)
            if (config("subdatasetName").nonEmpty) println(s"::: resolved subdataset :::")
            if (verboseLevel > 1) resolvedDf.limit(1).show()

            // (3) set srid (if directed)
            // - this may throw an exception, e.g. Zarr or Zips
            // - metadata cache handled in the function
            sridDf = handleSRID(resolvedDf, config, verboseLevel)
            if (config("srid").toInt > 0) println(s"::: handled srid :::")
            if (verboseLevel > 1) sridDf.limit(1).show()

            // (4) increase nPartitions for retile and tessellate
            nPartitions = Math.min(10000, paths.length * 32)
            if (verboseLevel > 0) println(s"::: adjusted nPartitions to $nPartitions :::")

            // (5) retile with 'tileSize'
            // - different than RETILE (AKA SUBDIVIDE) read strategy
            // - metadata cache handled in the function
            retiledDf = retileRaster(sridDf, config, verboseLevel)
            if (config("retile").toBoolean) println(s"::: retiled (using 'tileSize') :::")
            if (verboseLevel > 1) retiledDf.limit(1).show()

            // (6) tessellation
            // - uses checkpoint dir
            // - optionally, skip project for data without SRS,
            //   e.g. Zarr handling (handled as WGS84)
            val skipProject = config("skipProject").toBoolean
            tessellatedDf = retiledDf
                .withColumn(
                    "tile",
                    rst_tessellate(col("tile"), lit(0), lit(skipProject))
                )
                .cache()
            var tessellatedDfCnt = tessellatedDf.count()
            Try(retiledDf.unpersist()) // <- let go of prior caching
            if (verboseLevel > 0) println(s"... tessellated at resolution 0 - count? $tessellatedDfCnt " +
                s"(going to $resolution) | skipProject? $skipProject")

            var tmpTessellatedDf: DataFrame = null
            if (resolution > 0) {
                for (res <- 1 to resolution) {
                    tmpTessellatedDf = tessellatedDf
                        .withColumn(
                            s"tile_$res",
                            rst_tessellate(col("tile"), lit(res), lit(skipProject)) // <- skipProject needed?
                        )
                        .drop("tile")
                        .filter(col(s"tile_$res").isNotNull)
                        .withColumnRenamed(s"tile_$res", "tile")
                        .cache()                                // <- cache tmp
                    tessellatedDfCnt = tmpTessellatedDf.count() // <- count tmp (before unpersist)
                    Try(tessellatedDf.unpersist())              // <- uncache existing tessellatedDf
                    tessellatedDf = tmpTessellatedDf            // <- assign tessellatedDf
                    if (verboseLevel > 0) println(s"... tessellated at resolution $res - count? $tessellatedDfCnt " +
                        s"(going to $resolution) | skipProject? $skipProject")
                }
            }
            println(s"::: tessellated :::")
            if (verboseLevel > 1) tessellatedDf.limit(1).show()

            if (config("stopAtTessellate").toBoolean) {
                // return tessellated
                tessellatedDf
            } else {
                // (7) combine
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
                Try(tessellatedDf.unpersist())
                println(s"::: combined (${config("combiner")}) - count? $combinedDfCnt :::")
                if (verboseLevel > 1) combinedDf.limit(1).show()

                // (8) band exploded
                validDf = combinedDf
                    .filter(size(col("grid_measures")) > lit(0))
                    .select(
                        posexplode(col("grid_measures")).as(Seq("band_id", "measure")),
                        col("tile").getField("index_id").alias("cell_id")
                    )
                    .select(
                        col("band_id"),
                        col("cell_id"),
                        col("measure")
                    )
                    .cache()
                val validDfCnt = validDf.count()
                invalidDf = combinedDf
                    .filter(size(col("grid_measures")) === lit(0))
                    .select(
                        lit(0).alias("band_id"),
                        lit(0.0).alias("measure"),
                        col("tile").getField("index_id").alias("cell_id")
                    )
                    .select(
                        col("band_id"),
                        col("cell_id"),
                        col("measure")
                    )
                    .cache()
                val invalidDfCnt = invalidDf.count()
                Try(combinedDf.unpersist())
                val hasValid = validDfCnt > 0
                println(s"::: band exploded (if needed) - valid count? $validDfCnt, invalid count? $invalidDfCnt :::")
                bandDf =
                    if (hasValid) validDf
                    else invalidDf
                if (verboseLevel > 1) bandDf.limit(1).show()

                // (9) handle k-ring resample
                // - metadata cache handled in the function
                kSampleDf = kRingResample(bandDf, config, verboseLevel).cache()
                if (config("kRingInterpolate").toInt > 0) println(s"::: k-ring resampled :::")
                if (verboseLevel > 1) kSampleDf.limit(1).show()

                kSampleDf // <- returned cached (this is metadata only)
            }
        } finally {
            Try(pathsDf.unpersist())
            Try(resolvedDf.unpersist())
            Try(sridDf.unpersist())
            Try(retiledDf.unpersist())
            //Try(tessellatedDf.unpersist())
            Try(combinedDf.unpersist())
            Try(bandDf.unpersist())
            Try(validDf.unpersist())
            Try(invalidDf.unpersist())
        }
    }

    /**
     * Resolve the subdatasets if configured to do so. Resolving subdatasets
     * requires "subdatasetName" to be set.
     *
     * @param df
     *   The DataFrame containing the paths.
     * @param config
     *   The configuration map.
     * @param verboseLevel
     *   Whether to print interim results (0,1,2).
     * @return
     *   The DataFrame after handling.
     */
    private def resolveSubdataset(df: DataFrame, config: Map[String, String], verboseLevel: Int) = {
        val subdatasetName = config("subdatasetName")
        if (subdatasetName.nonEmpty) {
            if (verboseLevel > 0) println(s"... subdataset? = $subdatasetName")
            val result = df
                .withColumn("subdatasets", rst_subdatasets(col("tile")))
                .withColumn("tile", rst_separatebands(col("tile")))
                .withColumn("tile", rst_getsubdataset(col("tile"), lit(subdatasetName)))
                .cache()
            val cnt = result.count() // <- need this to force cache
            if (verboseLevel > 0) println(s"... count? $cnt")
            Try(df.unpersist())      // <- uncache df (after count)
            result
        } else {
            df                       // <- keep cached
        }
    }

    /**
     * Attempt to set srid.
     * - Some drivers don't support this, e.g. Zarr might not.
     * - Won't attempt for zip files.
     *
     * @param df
     *   The DataFrame containing the paths.
     * @param config
     *   The configuration map.
     * @param verboseLevel
     *   Whether to print interim results (0,1,2).
     * @return
     *   The DataFrame after handling.
     */
    private def handleSRID(df: DataFrame, config: Map[String, String], verboseLevel: Int) = {
        val srid = config("srid").toInt
        if (srid > 0) {
            if (verboseLevel > 0) println(s"... srid? = $srid")
            val result = df
                .withColumn("tile", rst_setsrid(col("tile"), lit(srid))) // <- this seems to be required
                .cache()
            val cnt = result.count() // <- need this to force cache
            if (verboseLevel > 0) println(s"... count? $cnt")
            Try(df.unpersist())      // <- uncache df (after count)
            result
        } else {
            df                       // <- keep cached
        }
    }

    /**
      * Retile the tile if configured to do so. Retiling requires "retile" to
      * be set to true in the configuration map. It also requires "tileSize" to
      * be set to the desired tile size.
      *
      * @param df
      *   The DataFrame containing the rasters.
      * @param config
      *   The configuration map.
      * @param verboseLevel
      *   Whether to print interim results (0,1,2).
      * @return
      *   The DataFrame after handling.
      */
    private def retileRaster(df: DataFrame, config: Map[String, String], verboseLevel: Int) = {
        val isRetile = config.getOrElse("retile", "false").toBoolean
        val tileSize = config.getOrElse("tileSize", "-1").toInt

        if (isRetile && tileSize > 0) {
            if (verboseLevel > 0) println(s"... retiling to tileSize = $tileSize")
            val result = df
                .withColumn("tile", rst_retile(col("tile"), lit(tileSize), lit(tileSize)))
                .repartition(nPartitions)
                .cache()
            val cnt = result.count() // <- need this to force cache
            if (verboseLevel > 0) println(s"... count? $cnt")
            Try(df.unpersist())      // <- uncache df (after count)
            result
        } else {
            df                       // <- keep cached
        }
    }

    /**
      * Interpolate the grid using the k ring size if configured to do so.
      * Interpolation requires "kRingInterpolate" to be set to the desired k
      * ring size in the configuration map. If "kRingInterpolate" is set to a
      * value greater than 0, the grid will be interpolated using the k ring
      * size. Otherwise, the grid will be returned as is. The interpolation is
      * done using the inverse distance weighted sum of the k ring cells.
      * @param df
      *   The DataFrame containing the grid.
      * @param config
      *   The configuration map.
      * @param verboseLevel
      *   Whether to print interim results (0,1,2).
      * @return
      *   The DataFrame after handling.
      */
    private def kRingResample(df: DataFrame, config: Map[String, String], verboseLevel: Int) = {
        val k = config.getOrElse("kRingInterpolate", "0").toInt

        def weighted_sum(measureCol: String, weightCol: String) = {
            sum(col(measureCol) * col(weightCol)) / sum(col(weightCol))
        }.alias(measureCol)

        if (k > 0) {
            if (verboseLevel > 0) println(s"... kRingInterpolate = $k rings")
            val result = df
                .withColumn("origin_cell_id", col("cell_id"))
                .withColumn("cell_id", explode(grid_cellkring(col("origin_cell_id"), k)))
                .repartition(nPartitions)
                .withColumn("weight", lit(k + 1) - grid_distance(col("origin_cell_id"), col("cell_id")))
                .groupBy("band_id", "cell_id")
                .agg(weighted_sum("measure", "weight"))
                .cache()
            val cnt = result.count() // <- need this to force cache
            if (verboseLevel > 0) println(s"... count? $cnt")
            Try(df.unpersist())      // <- uncache df (after count)
            result
        } else {
            df                       // <- keep cached
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
            "combiner" -> this.extraOptions.getOrElse("combiner", "mean"),
            "driverName" -> this.extraOptions.getOrElse("driverName", ""),
            "extensions" -> this.extraOptions.getOrElse("extensions", "*"),
            "kRingInterpolate" -> this.extraOptions.getOrElse("kRingInterpolate", "0"),
            "nPartitions" -> this.extraOptions.getOrElse("nPartitions", sparkSession.conf.get("spark.sql.shuffle.partitions")),
            "resolution" -> this.extraOptions.getOrElse("resolution", "0"),
            "retile" -> this.extraOptions.getOrElse("retile", "false"),
            "srid" -> this.extraOptions.getOrElse("srid", "0"),
            "sizeInMB" -> this.extraOptions.getOrElse("sizeInMB", "0"),
            "skipProject" -> this.extraOptions.getOrElse("skipProject", "false"),
            "stopAtTessellate" -> this.extraOptions.getOrElse("stopAtTessellate", "false"),
            "subdatasetName" -> this.extraOptions.getOrElse("subdatasetName", ""),
            "tileSize" -> this.extraOptions.getOrElse("tileSize", "512"),
            "uriDeepCheck" -> this.extraOptions.getOrElse("uriDeepCheck", "false"),
            "verboseLevel" -> this.extraOptions.getOrElse("verboseLevel", "0"),
            "vsizip" -> this.extraOptions.getOrElse("vsizip", "false")
        )
    }
    // scalastyle:on println

}
