package com.databricks.labs.mosaic.datasource.multiread

import com.databricks.labs.mosaic.{MOSAIC_RASTER_READ_AS_PATH, MOSAIC_RASTER_READ_STRATEGY, MOSAIC_RASTER_SUBDIVIDE_ON_READ, NO_EXT, POLYGON_EMPTY_WKT}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.utils.{FileUtils, PathUtils}
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

    private var nPartitions = -1                       // <- may change

    private var readStrat = MOSAIC_RASTER_READ_AS_PATH // <- may change

    private var phases = Seq("path", "subdataset", "srid", "retile", "tessellate", "combine", "interpolate")

    private var interimTbls = Seq.empty[String]

    private var doTables = false                              // <- may change

    private var keepInterimTables = false                     // <- may change

    private var rasterToGridCombiner: Column => Column = _    // <- will change

    override def load(path: String): DataFrame = load(Seq(path): _*)

    override def load(paths: String*): DataFrame = {

        println("\n<<< raster_to_grid invoked >>>")

        // <<< CONFIG >>>
        // - turn off aqe coalesce partitions for this op
        var config = getConfig
        val verboseLevel = config("verboseLevel").toInt

        doTables = config("finalTableFqn").nonEmpty
        keepInterimTables = config("keepInterimTables").toBoolean
        nPartitions = config("nPartitions").toInt
        rasterToGridCombiner = getRasterToGridFunc(config("combiner")) // <- want to fail early

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
        readStrat = {
            // have to go out of way to specify "-1"
            // don't use subdivide strategy with zips (AKA MOSAIC_RASTER_SUBDIVIDE_ON_READ)
            if (config("sizeInMB").toInt < 0 || config("vsizip").toBoolean) MOSAIC_RASTER_READ_AS_PATH
            else MOSAIC_RASTER_SUBDIVIDE_ON_READ
        }

        if (verboseLevel > 0) println(
            s"raster_to_grid -> nestedHandling? $nestedHandling | nPartitions? $nPartitions | read strat? $readStrat"
        )
        if (verboseLevel > 1) println(s"\nraster_to_grid - config (after any reader mods)? $config\n")

        val baseOptions = Map(
            "extensions" -> config("extensions"),
            "vsizip" -> config("vsizip"),
            "subdatasetName" -> config("subdatasetName"),
            MOSAIC_RASTER_READ_STRATEGY -> readStrat
        )
        val readOptions =
            if (driverName.nonEmpty && readStrat == MOSAIC_RASTER_SUBDIVIDE_ON_READ) {
                baseOptions +
                    ("driverName" -> driverName, "sizeInMB" -> config("sizeInMB"))
            }
            else if (driverName.nonEmpty) baseOptions + ("driverName" -> driverName)
            else if (readStrat == MOSAIC_RASTER_SUBDIVIDE_ON_READ) baseOptions + ("sizeInMB" -> config("sizeInMB"))
            else baseOptions
        if (verboseLevel > 1) println(s"\nraster_to_grid - readOptions? $readOptions\n")

        // <<< PERFORM READ >>>
        var pathsDf: DataFrame = null
        var resolvedDf: DataFrame = null
        var sridDf: DataFrame = null
        var retiledDf: DataFrame = null
        var tessellatedDf: DataFrame = null
        var combinedDf: DataFrame = null
        var kSampleDf: DataFrame = null

        try {
            // (1) gdal reader load
            pathsDf = sparkSession.read
                .format("gdal")
                .options(readOptions)
                .load(paths: _*)
            if (doTables) {
                pathsDf = writeTable(pathsDf, "path", config, verboseLevel)
            } else {
                pathsDf = pathsDf
                    .repartition(nPartitions)
                    .cache()
            }
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

            // (4) increase nPartitions for retile and tessellate
            nPartitions = Math.min(10000, pathsDfCnt * 32).toInt
            if (verboseLevel > 0 && !doTables) println(s"::: adjusted nPartitions to $nPartitions :::")

            // (5) retile with 'tileSize'
            // - different than RETILE (AKA SUBDIVIDE) read strategy
            // - metadata cache handled in the function
            retiledDf = retileRaster(sridDf, config, verboseLevel)

            // (6) tessellation
            // - uses checkpoint dir
            // - optionally, skip project for data without SRS,
            //   e.g. Zarr handling (handled as WGS84)
            tessellatedDf = tessellate(retiledDf, config, verboseLevel)

            if (config("stopAtTessellate").toBoolean) {
                // return tessellated
                tessellatedDf
            } else {
                // (7) combine
                combinedDf = combine(tessellatedDf, config, verboseLevel)

                // (8) handle k-ring resample
                // - metadata cache handled in the function
                kSampleDf = kRingResample(combinedDf, config, verboseLevel)

                kSampleDf // <- returned cached (this is metadata only)
            }
        } finally {
            // handle interim tables
            deleteInterimTables(config, verboseLevel)

            // handle interim dfs
            if (!doTables) {
                Try(pathsDf.unpersist())
                Try(resolvedDf.unpersist())
                Try(sridDf.unpersist())
                Try(retiledDf.unpersist())
                if (!config("stopAtTessellate").toBoolean) Try(tessellatedDf.unpersist())
                Try(combinedDf.unpersist())
            }
        }
    }

    /**
     * Resolve the subdatasets if configured to do so. Resolving subdatasets
     * - requires "subdatasetName" to be set.
     * - Skips if read strategy is [[MOSAIC_RASTER_SUBDIVIDE_ON_READ]].
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
        if (subdatasetName.nonEmpty && readStrat != MOSAIC_RASTER_SUBDIVIDE_ON_READ) {
            if (verboseLevel > 0) println(s"... subdataset? = $subdatasetName")
            var result = df
                .withColumn("subdatasets", rst_subdatasets(col("tile")))
                .withColumn("tile", rst_separatebands(col("tile")))
                .withColumn("tile", rst_getsubdataset(col("tile"), lit(subdatasetName)))
            if (doTables) {
                result = writeTable(result, "subdataset", config, verboseLevel)
            } else {
                result.cache()
            }
            val cnt = result.count() // <- need this to force cache
            if (verboseLevel > 0) println(s"... count? $cnt")
            if (!doTables) {
                FileUtils.deleteDfTilePathDirs(df, verboseLevel = verboseLevel, msg = "df (after subdataset)")
                Try(df.unpersist())      // <- uncache df (after count)
            }

            result
        } else {
            df                          // <- keep as-is
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
            var result = df
                .withColumn("tile", rst_setsrid(col("tile"), lit(srid)))
            if (doTables) {
                result = writeTable(result, "srid", config, verboseLevel)
            } else result.cache()
            val cnt = result.count() // <- need this to force cache
            if (verboseLevel > 0) println(s"... count? $cnt")
            if (!doTables) {
                FileUtils.deleteDfTilePathDirs(df, verboseLevel = verboseLevel, msg = "df (after srid)")
                Try(df.unpersist()) // <- uncache df (after count)
            }
            println(s"::: handled srid :::")
            if (verboseLevel > 1) result.limit(1).show()

            result
        } else {
            df                       // <- as-is
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
            var result = df
                .withColumn("tile", rst_retile(col("tile"), lit(tileSize), lit(tileSize)))
            if (doTables) {
                result = writeTable(result, "retile", config, verboseLevel)
            } else {
                result = result
                    .repartition(nPartitions)
                    .cache()
            }
            val cnt = result.count() // <- need this to force cache
            if (verboseLevel > 0) println(s"... count? $cnt")
            if (!doTables) {
                FileUtils.deleteDfTilePathDirs(df, verboseLevel = verboseLevel, msg = "df (after retile)")
                Try(df.unpersist()) // <- uncache df (after count)
            }
            println(s"::: retiled (using 'tileSize') :::")
            if (verboseLevel > 1) result.limit(1).show()

            result
        } else {
            df                       // <- as-is
        }
    }

    /**
     * Perform tessellation on the DataFrame.
     * - for table writes, generates a table per resolution.
     *
     * @param df
     *   The DataFrame to tessellate.
     * @param config
     *   The configuration map.
     * @param verboseLevel
     *   Whether to print interim results (0,1,2).
     * @return
     *   The DataFrame after handling.
     */
    private def tessellate(df: DataFrame, config: Map[String, String], verboseLevel: Int): DataFrame = {
        val resolution = config("resolution").toInt
        val limitTessellate = config("limitTessellate").toInt
        val skipProject = config("skipProject").toBoolean

        // [1] initially tessellate at res=0
        var tessellatedDf = df
            .withColumn(
                "tile",
                rst_tessellate(col("tile"), lit(0), lit(skipProject))
            )
        if (limitTessellate > 0) {
            // handle optional limit (for testing)
            tessellatedDf = tessellatedDf.limit(limitTessellate)
        }
        if (doTables) {
            tessellatedDf = writeTable(
                tessellatedDf,
                "tessellate",
                config,
                verboseLevel,
                overrideTblName = s"${config("finalTableFqn")}_tessellate_0"
            )
        } else {
            tessellatedDf = tessellatedDf.cache()
        }
        var tessellatedDfCnt = tessellatedDf.count()
        if (!doTables) Try(df.unpersist()) // <- let go of prior caching

        if (verboseLevel > 0) println(s"... tessellated at resolution 0 - count? $tessellatedDfCnt " +
            s"(going to $resolution) | skipProject? $skipProject")

        var tmpTessellatedDf: DataFrame = null
        if (resolution > 0) {
            // [2] iterate over remainined resolutions
            for (res <- 1 to resolution) {
                tmpTessellatedDf = tessellatedDf
                    .withColumn(
                        s"tile_$res",
                        rst_tessellate(col("tile"), lit(res), lit(skipProject)) // <- skipProject needed?
                    )
                    .drop("tile")
                    .filter(col(s"tile_$res").isNotNull)
                    .withColumnRenamed(s"tile_$res", "tile")
                if (limitTessellate > 0) {
                    // handle optional limit (for testing)
                    tmpTessellatedDf = tmpTessellatedDf.limit(limitTessellate)
                }
                if (doTables) {
                    tmpTessellatedDf = writeTable(
                        tessellatedDf,
                        "tessellate",
                        config,
                        verboseLevel,
                        overrideTblName = s"${config("finalTableFqn")}_tessellate_$res"
                    )
                } else {
                    tmpTessellatedDf = tmpTessellatedDf.cache()  // <- cache tmp
                    tmpTessellatedDf.count()           // <- count tmp (before unpersist)
                    FileUtils.deleteDfTilePathDirs(tessellatedDf, verboseLevel = verboseLevel, msg = s"tessellatedDf (res=$res)")
                    Try(tessellatedDf.unpersist())               // <- uncache existing tessellatedDf
                }
                tessellatedDf = tmpTessellatedDf                 // <- assign tessellatedDf
                tessellatedDfCnt = tessellatedDf.count()
                if (verboseLevel > 0) println(s"... tessellated at resolution $res - count? $tessellatedDfCnt " +
                    s"(going to $resolution) | skipProject? $skipProject")
            }
        }
        println(s"::: tessellated :::")
        if (verboseLevel > 1) tessellatedDf.limit(1).show()

        tessellatedDf
    }

    /**
     * Combine the tessellated DataFrame.
     *
     * @param df
     *   The DataFrame containing the grid.
     * @param config
     *   The configuration map.
     * @param verboseLevel
     *   Whether to print interim results (0,1,2).
     * @return
     *   The DataFrame after handling.
     */
    private def combine(df: DataFrame, config: Map[String, String], verboseLevel: Int): DataFrame = {

        val combinedDf = df
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
        if (!doTables) {
            FileUtils.deleteDfTilePathDirs(df, verboseLevel = verboseLevel, msg = "tessellatedDf")
            Try(df.unpersist())
        }
        println(s"::: combined (${config("combiner")}) - count? $combinedDfCnt :::")
        if (verboseLevel > 1) combinedDf.limit(1).show()

        var validDf: DataFrame = null
        var invalidDf: DataFrame = null
        try {
            // band exploded (after combined)
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
            println(s"::: band exploded (if needed) - valid count? $validDfCnt, invalid count? $invalidDfCnt :::")
            var result =
                if (validDfCnt > 0) validDf
                else invalidDf
            if (doTables) {
                result = writeTable(result, "combine", config, verboseLevel)
            }
            if (verboseLevel > 1) result.limit(1).show()

            result
        } finally {
            Try(combinedDf.unpersist())
            Try(validDf.unpersist())
            Try(invalidDf.unpersist())
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
            var result = df
                .withColumn("origin_cell_id", col("cell_id"))
                .withColumn("cell_id", explode(grid_cellkring(col("origin_cell_id"), k)))
                .withColumn("weight", lit(k + 1) - grid_distance(col("origin_cell_id"), col("cell_id")))
                .groupBy("band_id", "cell_id")
                .agg(weighted_sum("measure", "weight"))
            if (doTables) {
                result = writeTable(result, "interpolate", config, verboseLevel)
            } else result.cache()
            val cnt = result.count() // <- need this to force cache
            if (verboseLevel > 0) println(s"... count? $cnt")
            if (!doTables) {
                Try(df.unpersist()) // <- uncache df (after count)
            }
            println(s"::: k-ring resampled :::")
            if (verboseLevel > 1) result.limit(1).show()

            result
        } else {
            df                      // <- as-is
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
     * Attempt to parse the catalog from the "finalTableFqn".
     * - If fqn is empty, return None.
     * - If the fqn has the catalog, return that; if not, return current catalog.
     *
     * @param config
     *   The config to use.
     * @return
     *   Option string.
     */
    private def getCatalog(config: Map[String, String]): Option[String] = {
        val fqn = config("finalTableFqn")
        if (fqn.isEmpty) None
        else {
            val parts = fqn.split(".")
            if (parts.length == 3) Some(parts(0))             // <- catalog provided
            else Some(sparkSession.catalog.currentCatalog())  // <- current catalog
        }
    }

    /**
     * Attempt to parse the schema from the "finalTableFqn".
     * - If fqn is empty, return None.
     * - If the fqn has the schema, return that; if not, return current schema.
     *
     * @param config
     *   The config to use.
     * @return
     *   Option string.
     */
    private def getSchema(config: Map[String, String]): Option[String] = {
        val fqn = config("finalTableFqn")
        if (fqn.isEmpty) None
        else {
            val parts = fqn.split(".")
            if (parts.length == 3) Some(parts(1))             // <- catalog + schema provided
            if (parts.length == 2) Some(parts(0))             // <- schema provided
            else Some(sparkSession.catalog.currentDatabase)   // <- current schema
        }
    }

    /**
     * Write DataFrame to Delta Lake.
     * - uses the fqn for catalog and schema.
     * - uses the fqn for interim tables.
     * - uses the config for "deltaFileMB".
     * - uses the "cellid" col to liquid cluster in tessellate, combine, and interpolate phases.
     * - adds interim table names to the `interimTbls` array.
     *
     * @param df
     *   DataFrame to write.
     * @param phase
     *   Phase of processing: "path", "subdataset", "srid", "retile", "tessellate", "combine", "interpolate"
     * @param config
     *   The configuration map.
     * @param verboseLevel
     *   Control printing interim results (0,1,2).
     * @return
     *   DataFrame of the table for the phase.
     */
    private def writeTable(
                              df: DataFrame,
                              phase: String,
                              config: Map[String, String],
                              verboseLevel: Int,
                              overrideTblName: String = ""
                          ): DataFrame = {
        // [1] table name and write mode
        var finalTbl = false
        val fqn =
            if (config("stopAtTessellate").toBoolean && phase == "tessellate") {
                finalTbl = true
                if (overrideTblName.nonEmpty) overrideTblName
                else config("finalTablefqn")
            }
            else if (config("kRingInterpolate").toInt == 0 && phase == "combine") {
                finalTbl = true
                if (overrideTblName.nonEmpty) overrideTblName
                else config("finalTablefqn")
            }
            else if (config("kRingInterpolate").toInt > 0 && phase == "interpolate") {
                finalTbl = true
                if (overrideTblName.nonEmpty) overrideTblName
                else config("finalTablefqn")
            } else {
                // interim table
                val tbl =
                    if (overrideTblName.nonEmpty) overrideTblName
                    else s"${config("finalTablefqn")}_$phase"
                interimTbls :+ tbl
                tbl
            }

        val finalDf =
            if (finalTbl && config("finalTableFuse").nonEmpty) {
                df
                    .withColumn("tile", rst_write(col("tile"), config("finalTableFuse")))
            } else df

        // [2] initial write of the table to delta lake
        // - this is an overwrite operation
        finalDf.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(fqn)

        // [3] change target for more files to spread out operation (SQL)
        sparkSession.sql(s"ALTER TABLE $fqn SET TBLPROPERTIES(delta.targetFileSize = '${config("deltaFileMB").toInt}mb')")

        // [4] set-up liquid clustering on tables with cellid (SQL)
        if (Seq("tessellate", "combine", "interpolate").contains(phase)) {
            sparkSession.sql(s"ALTER TABLE $fqn CLUSTER BY (cellid)")
        }

        // [5] perform optimize to enact the change(s) (SQL)
        sparkSession.sql(s"OPTIMIZE $fqn")

        // [6] return a dataframe of the table
        sparkSession.table(fqn)
    }

    /**
     * If config "keepInterimTables" is false, drop the tables in `keepInterimTbls`.
     * - Also, will delete the checkpoint files generated.
     *
     * @param config
     *   The configuration map.
     * @param verboseLevel
     *   Control printing interim results (0,1,2).
     */
    private def deleteInterimTables(config: Map[String, String], verboseLevel: Int): Unit = {
        if (!keepInterimTables) {
            for (tbl <- interimTbls) {
                // delete underlying file paths
                FileUtils.deleteDfTilePathDirs(
                    sparkSession.table(tbl),
                    verboseLevel = verboseLevel,
                    msg = tbl
                )
                // drop the table
                sparkSession.sql(s"DROP TABLE IF EXISTS $tbl")
            }
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
            "deltaFileMB" -> this.extraOptions.getOrElse("deltaFileMB", "8"),                 // <- for tables
            "driverName" -> this.extraOptions.getOrElse("driverName", ""),
            "extensions" -> this.extraOptions.getOrElse("extensions", "*"),
            "finalTableFqn" -> this.extraOptions.getOrElse("finalTableFqn", ""),              // <- identifies use of tables
            "finalTableFuse" -> this.extraOptions.getOrElse("finalTableFuse", ""),            // <- for tables
            "keepInterimTables" -> this.extraOptions.getOrElse("keepIterimTables", "false"),  // <- for tables
            "kRingInterpolate" -> this.extraOptions.getOrElse("kRingInterpolate", "0"),
            "limitTessellate" -> this.extraOptions.getOrElse("limitTessellate", "0"),
            "nPartitions" -> this.extraOptions.getOrElse("nPartitions", sparkSession.conf.get("spark.sql.shuffle.partitions")),
            "resolution" -> this.extraOptions.getOrElse("resolution", "0"),
            "retile" -> this.extraOptions.getOrElse("retile", "false"),
            "srid" -> this.extraOptions.getOrElse("srid", "0"),
            "sizeInMB" -> this.extraOptions.getOrElse("sizeInMB", "0"),
            "skipProject" -> this.extraOptions.getOrElse("skipProject", "false"),             // <- debugging primarily
            "stopAtTessellate" -> this.extraOptions.getOrElse("stopAtTessellate", "false"),   // <- debugging + tessellate perf
            "subdatasetName" -> this.extraOptions.getOrElse("subdatasetName", ""),
            "tileSize" -> this.extraOptions.getOrElse("tileSize", "512"),
            "uriDeepCheck" -> this.extraOptions.getOrElse("uriDeepCheck", "false"),
            "verboseLevel" -> this.extraOptions.getOrElse("verboseLevel", "0"),
            "vsizip" -> this.extraOptions.getOrElse("vsizip", "false")
        )
    }

}
