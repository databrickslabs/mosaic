package com.databricks.labs.mosaic.datasource.multiread

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.{
    MOSAIC_RASTER_READ_AS_PATH,
    MOSAIC_RASTER_READ_STRATEGY,
    MOSAIC_RASTER_SUBDIVIDE_ON_READ,
    NO_EXT
}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.utils.{FileUtils, PathUtils}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.nio.file.{Files, Paths}
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
    private val mc = MosaicContext.context()
    import mc.functions._

    private var nPartitions = -1                             // <- may change

    private var nestedHandling = false                       // <- may change

    private var readStrat = MOSAIC_RASTER_READ_AS_PATH       // <- may change

    private var config = Map.empty[String, String]           // <- may change

    private var readOptions = Map.empty[String, String]      // <- may change

    private var verboseLevel = 0                             // <- may change

    private val phases = Seq(
        "path", "tif", "retile", "tessellate", "combine", "interpolate" // <- ordered
    )

    private val tileCols = Seq(
        "tess_tile", "re_tile", "tile", "orig_tile"                            // <- ordered
    )

    private var interimTbls = Seq.empty[String]

    private var doTables = false                              // <- may change

    private var keepInterimTables = false                     // <- may change

    private var rasterToGridCombiner: Column => Column = _    // <- will change

    override def load(path: String): DataFrame = load(Seq(path): _*)

    override def load(paths: String*): DataFrame = {

        logMsg("\n<<< raster_to_grid invoked >>>", 0)

        // <<< CONFIG >>>
        // - turn off aqe coalesce partitions for this op
        config = getConfig
        verboseLevel = config("verboseLevel").toInt
        doTables = config("finalTableFqn").nonEmpty
        keepInterimTables = config("keepInterimTables").toBoolean
        nPartitions = config("nPartitions").toInt
        rasterToGridCombiner = getRasterToGridFunc(config("combiner")) // <- want to fail early

        sparkSession.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
        logMsg(s"raster_to_grid -> 'spark.sql.adaptive.coalescePartitions.enabled' set to false", 1)

        // <<< SETUP POST-CONFIG>>>
        setupPostConfig(paths: _*)

        // <<< CLEAN-UP PRIOR TABLES >>>
        // - if `doTables` is true
        cleanUpPriorTables()

        // <<< PERFORM READ >>>
        var pathsDf: DataFrame = null
        var convertDf: DataFrame = null
        var retiledDf: DataFrame = null
        var tessellatedDf: DataFrame = null
        var combinedDf: DataFrame = null
        var kSampleDf: DataFrame = null

        try {
            // (1) gdal reader load
            pathsDf = initialLoad(paths: _*)

            // (2) toTif conversion (if directed)
            convertDf = convertToTif(pathsDf)

            // (3) increase nPartitions for retile and tessellate
            nPartitions = Math.min(10000, pathsDf.count() * 32).toInt
            logMsg(s"::: adjusted nPartitions to $nPartitions :::", 1)

            // (4) retile with 'tileSize'
            // - different than RETILE (AKA SUBDIVIDE) read strategy
            retiledDf = retileRaster(convertDf)

            // (5) tessellation
            // - uses checkpoint dir
            // - optionally, skip project for data without SRS,
            //   e.g. Zarr handling (handled as WGS84)
            tessellatedDf = tessellate(retiledDf)

            if (config("stopAtTessellate").toBoolean) {
                // return tessellated
                tessellatedDf
            } else {
                // (6) combine
                combinedDf = combine(tessellatedDf)

                // (7) handle k-ring resample
                kSampleDf = kRingResample(combinedDf)

                kSampleDf // <- returned cached (this is metadata only)
            }
        } finally {
            // handle interim tables (if relevant)
            deleteInterimTables()

            // handle interim dfs
            if (!doTables) {
                Try(pathsDf.unpersist())
                Try(convertDf.unpersist())
                Try(retiledDf.unpersist())
                if (!config("stopAtTessellate").toBoolean) Try(tessellatedDf.unpersist())
                Try(combinedDf.unpersist())
            }
        }
    }

    /**
     * Initial load using "gdal" reader.
     *
     * @param paths
     *   The paths to load, e.g. a directory or list of files.
     * @return
     *   The DataFrame after handling.
     */
    private def initialLoad(paths: String*): DataFrame = {
        var result = sparkSession.read
            .format("gdal")
            .options(readOptions)
            .load(paths: _*)
        if (doTables) {
            result = writeTable(result, "path")
                .repartition(nPartitions)
        } else {
            result = result
                .repartition(nPartitions)
                .cache()
        }
        val pathsDfCnt = result.count()
        logMsg(s"::: gdal reader loaded - count? $pathsDfCnt :::", 0)
        if (verboseLevel >= 2) result.limit(1).show()

        result
    }

    /**
     * Convert to tif.
     * - Generates tif variation of 'tile' (if directed).
     * - 'tile' column becomes 'orig_tile'.
     *
     * @param df
     *   The df to act on.
     * @return
     *   The DataFrame after handling.
     */
    private def convertToTif(df: DataFrame): DataFrame = {
        val toTif = config("toTif").toBoolean
        if (toTif) {
            val toFuseDir =
                if (config("finalTableFuse").nonEmpty) config("finalTableFuse")
                else GDAL.getCheckpointDir
            var result = df
                .withColumnRenamed("tile", "orig_tile")
                .filter(col("orig_tile").isNotNull) // <- keep non-nulls only
                .withColumn("tile", rst_totif(col("orig_tile"), toFuseDir))
            if (doTables) {
                result = writeTable(result, "tif")
                    .repartition(nPartitions)
            } else {
                result = result.cache()
            }
            val cnt = result.count() // <- need this to force cache
            logMsg(s"\t... count? $cnt", 1)
            cleanUpDfFiles(df, "tif")
            logMsg(s"::: converted to tif :::", 0)
            if (verboseLevel >= 2) result.limit(1).show()

            result
        } else {
            df.withColumnRenamed("tile", "orig_tile")  // <- keep as-is
        }
    }

    /**
      * Retile the tile if configured to do so. Retiling requires "retile" to
      * be set to true in the configuration map. It also requires "tileSize" to
      * be set to the desired tile size; uses the best column based on prior
      * processing phases.
      *
      * @param df
      *   The DataFrame containing the rasters.
      * @return
      *   The DataFrame after handling.
      */
    private def retileRaster(df: DataFrame): DataFrame = {
        val isRetile = config.getOrElse("retile", "false").toBoolean
        val tileSize = config.getOrElse("tileSize", "-1").toInt

        if (isRetile && tileSize > 0) {
            logMsg(s"\t... retiling to tileSize = $tileSize", 1)
            val tileCol = bestTileCol(df, "retile")
            var result: DataFrame = df.select("*")
            result = result
                .filter(col(tileCol).isNotNull)
                .withColumn("re_tile", rst_retile(col(tileCol), lit(tileSize), lit(tileSize)))
            if (doTables) {
                result = writeTable(result, "retile")
                    .repartition(nPartitions)
            } else {
                result = result
                    .repartition(nPartitions)
                    .cache()
            }
            val cnt = result.count() // <- need this to force cache
            logMsg(s"\t... count? $cnt", 1)
            cleanUpDfFiles(df, "retile")
            logMsg(s"::: retiled (using 'tileSize') :::", 0)
            if (verboseLevel >= 2) result.limit(1).show()

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
     * @return
     *   The DataFrame after handling.
     */
    private def tessellate(df: DataFrame): DataFrame = {
        val resolution = config("resolution").toInt
        val limitTessellate = config("limitTessellate").toInt
        val skipProject = config("skipProject").toBoolean
        val stepTessellate = config("stepTessellate").toBoolean
        val initRes =
            if (stepTessellate) 0
            else resolution

        // [1] initially tessellate at res=0
        // - pick the preferred column to use
        val tileCol = bestTileCol(df, "tessellate")
        var tessellatedDf = df.withColumn("resolution", lit(initRes))
        tessellatedDf = tessellatedDf
            .filter(col(tileCol).isNotNull)
            .withColumn(
                "tess_tile",
                rst_tessellate(col(tileCol), col("resolution"), lit(skipProject))
            )
            .filter(col("tess_tile").isNotNull)
            .withColumn("cell_id", col("tess_tile.index_id"))
            .withColumnRenamed("path", "path_original")
            .withColumnRenamed("modificationTime", "modification_time_original")
            .withColumnRenamed("uuid", "uuid_original")
            .withColumnRenamed("srid", "srid_original")
            .drop("x_size", "y_size", "metadata", "subdatasets", "length")

        val tessCols = Array("cell_id", "resolution", "tess_tile") ++ tessellatedDf.columns
            .filter(c => c != "tess_tile" && c != "cell_id" && c != "resolution")
        tessellatedDf = tessellatedDf.selectExpr(tessCols : _*)
        if (limitTessellate > 0) {
            // handle optional limit (for testing)
            tessellatedDf = tessellatedDf.limit(limitTessellate)
        }
        if (doTables) {
            val tblName =
                if (stepTessellate && resolution > 0) s"${config("finalTableFqn")}_tessellate_0"
                else "" // <- default to phase name
            tessellatedDf = writeTable(
                tessellatedDf,
                "tessellate",
                overrideTblName = tblName
            )
                .repartition(nPartitions)
        } else {
            tessellatedDf = tessellatedDf.cache()
        }
        var tessellatedDfCnt = tessellatedDf.count()
        cleanUpDfFiles(df, "tessellate", msg = "resolution=0")

        logMsg(s"\t... tessellated at resolution $initRes - count? $tessellatedDfCnt " +
            s"(going to $resolution) | skipProject? $skipProject", 1)

        var tmpTessellatedDf: DataFrame = null
        if (stepTessellate && resolution > 0) {
            // [2] iterate over remaining resolutions
            for (res <- 1 to resolution) {
                tmpTessellatedDf = tessellatedDf
                    .withColumn("resolution", lit(res))
                    .withColumn(
                        s"tess_tile",
                        rst_tessellate(col("tess_tile"), col("resolution"), lit(skipProject)) // <- skipProject needed?
                    )
                    .filter(col("tess_tile").isNotNull)
                    .withColumn("cell_id", col("tess_tile.index_id"))
                tmpTessellatedDf = tmpTessellatedDf.selectExpr(tessCols : _*)
                if (limitTessellate > 0) {
                    // handle optional limit (for testing)
                    tmpTessellatedDf = tmpTessellatedDf.limit(limitTessellate)
                }
                if (doTables) {
                    tmpTessellatedDf = writeTable(
                        tessellatedDf,
                        "tessellate",
                        overrideTblName = s"${config("finalTableFqn")}_tessellate_$res"
                    )
                        .repartition(nPartitions)
                } else {
                    tmpTessellatedDf = tmpTessellatedDf.cache()  // <- cache tmp
                    tmpTessellatedDf.count()                     // <- count tmp (before unpersist)
                }
                cleanUpDfFiles(tessellatedDf, "tessellate", msg = s"resolution=$res")
                tessellatedDf = tmpTessellatedDf                 // <- assign tessellatedDf
                tessellatedDfCnt = tessellatedDf.count()
                logMsg(s"\t... tessellated at resolution $res - count? $tessellatedDfCnt " +
                    s"(going to $resolution) | skipProject? $skipProject", 1)
            }
        }
        logMsg(s"::: tessellated :::", 0)
        if (verboseLevel >= 2) tessellatedDf.limit(1).show()

        tessellatedDf
    }

    /**
     * Combine the tessellated DataFrame.
     *
     * @param df
     *   The DataFrame containing the grid.
     * @return
     *   The DataFrame after handling.
     */
    private def combine(df: DataFrame): DataFrame = {

        var combinedDf = df.select("*")
        for (tileCol <- tileCols) {
            if (tileCol != "tess_tile" && combinedDf.columns.contains(tileCol)) {
                combinedDf = combinedDf.drop(tileCol)
            }
        }
        combinedDf = combinedDf
            .groupBy("cell_id")
            .agg(rst_combineavg_agg(col("tess_tile")).alias("tess_tile"))
            .withColumn(
                "grid_measures",
                rasterToGridCombiner(col("tess_tile"))
            )
            .selectExpr(
                "cell_id",
                "grid_measures",
                "tess_tile as tile"
            )
            .cache()
        val combinedDfCnt = combinedDf.count()
        cleanUpDfFiles(df, "combine")

        logMsg(s"::: combined (${config("combiner")}) - count? $combinedDfCnt :::", 0)
        if (verboseLevel >= 2) combinedDf.limit(1).show()

        var validDf: DataFrame = null
        var invalidDf: DataFrame = null
        try {
            // band exploded (after combined)
            validDf = combinedDf
                .filter(size(col("grid_measures")) > lit(0))
                .select(
                    col("cell_id"),
                    posexplode(col("grid_measures")).as(Seq("band_id", "measure"))
                )
                .select(
                    col("cell_id"),
                    col("band_id"),
                    col("measure")
                )
                .cache()
            val validDfCnt = validDf.count()
            invalidDf = combinedDf
                .filter(size(col("grid_measures")) === lit(0))
                .select(
                    col("cell_id"),
                    lit(0).alias("band_id"),
                    lit(0.0).alias("measure")
                )
                .cache()
            val invalidDfCnt = invalidDf.count()
            logMsg(s"::: band exploded (if needed) - valid count? $validDfCnt, invalid count? $invalidDfCnt :::", 0)
            var result =
                if (validDfCnt > 0) validDf.select("*").cache()
                else invalidDf.select("*").cache()
            val resultCnt = result.count()
            logMsg(s"combiner - final result count? $resultCnt", 2)

            if (doTables) {
                result = writeTable(result, "combine")
                    .repartition(nPartitions)
            }
            if (verboseLevel >= 2) result.limit(1).show()

            result
        } finally {
            Try(combinedDf.unpersist())
            Try(invalidDf.unpersist())
            Try(validDf.unpersist())
        }
    }

    /**
      * Interpolate the grid using the k ring size if configured to do so.
      * Interpolation requires "kRingInterpolate" to be set to the desired k
      * ring size in the configuration map. If "kRingInterpolate" is set to a
      * value greater than 0, the grid will be interpolated using the k ring
      * size. Otherwise, the grid will be returned as is. The interpolation is
      * done using the inverse distance weighted sum of the k ring cells.
      *
      * @param df
      *   The DataFrame containing the grid.
      * @return
      *   The DataFrame after handling.
      */
    private def kRingResample(df: DataFrame): DataFrame = {
        val k = config.getOrElse("kRingInterpolate", "0").toInt

        def weighted_sum(measureCol: String, weightCol: String): Column = {
            sum(col(measureCol) * col(weightCol)) / sum(col(weightCol))
        }.alias(measureCol)

        if (k > 0) {
            logMsg(s"\t... kRingInterpolate = $k rings", 1)
            var result = df
                .withColumn("origin_cell_id", col("cell_id"))
                .withColumn("cell_id", explode(grid_cellkring(col("origin_cell_id"), k)))
                .withColumn("weight", lit(k + 1) - grid_distance(col("origin_cell_id"), col("cell_id")))
                .groupBy("band_id", "cell_id")
                .agg(weighted_sum("measure", "weight"))
                .select(
                  "cell_id",
                  "band_id",
                  "measure"
                )
            if (doTables) {
                result = writeTable(result, "interpolate")
                    .repartition(nPartitions)
            } else {
                result = result.cache()
            }
            val cnt = result.count() // <- need this to force cache
            logMsg(s"\t... count? $cnt", 1)
            if (!doTables) {
                Try(df.unpersist()) // <- uncache df (after count)
            }
            logMsg(s"::: k-ring resampled :::", 0)
            if (verboseLevel >= 2) result.limit(1).show()

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
     * Write DataFrame to Delta Lake.
     * - uses the fqn for catalog and schema.
     * - uses the fqn for interim tables.
     * - uses the config for "deltaFileRecords".
     * - uses the "cell_id" col to liquid cluster in tessellate, combine, and interpolate phases.
     * - adds interim table names to the `interimTbls` array.
     *
     * @param df
     *   DataFrame to write.
     * @param phase
     *   Phase of processing: "path", "subdataset", "srid", "retile", "tessellate", "combine", "interpolate"
     * @param overrideTblName
     *   If not "", use the override table name instead of the convention.
     * @return
     *   DataFrame of the table for the phase.
     */
    private def writeTable(
                              df: DataFrame,
                              phase: String,
                              overrideTblName: String = ""
                          ): DataFrame = {
        // [1] table name and write mode
        var finalTbl = false
        val fqn =
            if (config("stopAtTessellate").toBoolean && phase == "tessellate") {
                finalTbl = true
                if (overrideTblName.nonEmpty) overrideTblName
                else config("finalTableFqn")
            }
            else if (config("kRingInterpolate").toInt == 0 && phase == "combine") {
                finalTbl = true
                if (overrideTblName.nonEmpty) overrideTblName
                else config("finalTableFqn")
            }
            else if (config("kRingInterpolate").toInt > 0 && phase == "interpolate") {
                finalTbl = true
                if (overrideTblName.nonEmpty) overrideTblName
                else config("finalTableFqn")
            } else {
                // interim table
                val tbl =
                    if (overrideTblName.nonEmpty) overrideTblName
                    else s"${config("finalTableFqn")}_$phase"
                interimTbls :+ tbl
                tbl
            }

        val finalDf =
            if (finalTbl && config("finalTableFuse").nonEmpty && phase == "tessellate") {
                // only write to fuse for tessellate phase when it is the final phase
                df
                    .withColumn("tile", rst_write(col("tile"), config("finalTableFuse")))
            } else df

        // [2] initial write of the table to delta lake
        // - this is an overwrite operation
        // .option("maxRecordsPerFile", "")
        val writeOpts =
            if (config("deltaFileRecords").toInt > 0) {
                Map(
                  "overwriteSchema" -> "true",
                  "maxRecordsPerFile" -> config("deltaFileRecords")
                )
            } else Map("overwriteSchema" -> "true")

        finalDf.write
            .format("delta")
            .mode("overwrite")
            .options(writeOpts)
            .saveAsTable(fqn)

        // [3] change target for more files to spread out operation (SQL)
        // - turn off auto optimize writes and auto compact
        sparkSession.sql(
            s"ALTER TABLE $fqn SET TBLPROPERTIES" +
                s"(" +
                s"delta.autoOptimize.optimizeWrite = false" +
                s", delta.autoOptimize.autoCompact = false" +
                s")"
        )

        // [6] return a dataframe of the table
        sparkSession.table(fqn)
    }

    /**
     * If config "keepInterimTables" is false, drop the tables in `keepInterimTbls`.
     * - Also, will delete the checkpoint files generated.
     */
    private def deleteInterimTables(): Unit = {
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
     * Clean-up any prior tables.
     * - This is an important call before each execution ("overwrite" alone doesn't work).
     * - Only if `doTables` is true.
     * - Only if they match the `finalTableFqn` [+ phase] name(s).
     */
    def cleanUpPriorTables(): Unit = {
        if (doTables) {
            val fqn = config("finalTableFqn")

            // [1] drop prior final table
            sparkSession.sql(s"DROP TABLE IF EXISTS $fqn")

            // [2] drop prior interim tables (where stepTessellate = false)
            for (phase <- phases) {
                sparkSession.sql(s"DROP TABLE IF EXISTS ${fqn}_$phase")
            }

            // [3] drop prior tessellate tables (where stepTessellate = true)
            for (res <- 0 to 15) {
                sparkSession.sql(s"DROP TABLE IF EXISTS ${fqn}_tessellate_$res")
            }
        }
    }

    /**
     * Clean-up df files.
     * - all possible tile columns.
     * - deletes files behind interim tables (if not keeping).
     * - un-caches df if not in table mode.
     *
     * @param df
     *   [[DataFrame]] to delete, may be backed by Delta Lake table.
     * @param phase
     *   Which phase is being handled.
     * @param msg
     *   If provided, print message to identify what is being deleted, default is "".
     */
    def cleanUpDfFiles(df: DataFrame, phase: String, msg: String = ""): Unit = {
        if (!doTables || !config("keepInterimTables").toBoolean) {
            for (tileCol <- tileCols) {
                if (df.columns.contains(tileCol)) {
                    val tileMsg =
                        if (msg.nonEmpty) s"'$tileCol' files (after '$phase') - $msg"
                        else s"'$tileCol' files (after '$phase')"
                    FileUtils.deleteDfTilePathDirs(
                        df, colName = tileCol, verboseLevel = verboseLevel, msg = tileMsg
                    )
                }
            }
            if (!doTables) Try(df.unpersist())
        }
    }

    /**
     * Identify the best tile column to use based on phase.
     * - test for presence of possible columns.
     * - test for non-null counts as needed.
     *
     * @param df
     *   [[DataFrame]] to test.
     * @param phase
     *   Phase of processing; only used when needed for logic.
     * @return
     *   Best column name.
     */
    def bestTileCol(df: DataFrame, phase: String): String = {
        if (
            phase == "tessellate" && df.columns.contains("re_tile")
                && df.filter(col("re_tile").isNotNull).count() > 0
        ) {
            "re_tile"
        } else if (
            (phase == "retile" || phase == "tessellate") && df.columns.contains("tile")
        ) {
            "tile"
        }
        else if (df.columns.contains("orig_tile")) "orig_tile" // <- available after 'tif' phase
        else "tile"                                            // <- catch-all
    }

    /**
     * Setup various varialbles after provided config.
     *
     * @param paths
     *   The paths to load, e.g. a directory or list of files.
     */
    private def setupPostConfig(paths: String*): Unit = {

        // <<< SETUP NESTED HANDLING >>>
        val nestedDrivers = Seq("hdf4", "hdf5", "grib", "netcdf", "zarr")
        val nestedExts = Seq("hdf4", "hdf5", "grb", "nc", "zarr")
        val driverName = config("driverName")

        nestedHandling = {
            if (config("vsizip").toBoolean || config("toTif").toBoolean) {
                false // <- skip subdivide for zips
            } else if (
                driverName.nonEmpty &&
                    nestedDrivers.contains(driverName.toLowerCase(Locale.ROOT))
            ) {
                logMsg(s"raster_to_grid -> config 'driverName' identified for nestedHandling ('$driverName')", 2)
                true
            } else if (
                config("extensions").split(";").map(p => p.trim.toLowerCase(Locale.ROOT))
                    .exists(nestedExts.contains)
            ) {
                logMsg(s"raster_to_grid -> config 'extensions' identified for nestedHandling ('${config("extensions")}')", 2)
                true
            } else if (
                paths.size == 1 && Files.isDirectory(Paths.get(paths(0)))
                    && Paths.get(paths(0)).toFile.listFiles().map(
                    p => PathUtils.getExtOptFromPath(p.getPath, None)
                        .getOrElse(NO_EXT)
                        .toLowerCase(Locale.ROOT)
                ).exists(p => nestedExts.contains(p.toLowerCase(Locale.ROOT)))
            ) {
                logMsg(s"raster_to_grid -> path ext (within dir) identified for nestedHandling", 2)
                true
            } else if (
                paths.map(
                        p => PathUtils.getExtOptFromPath(p, None)
                            .getOrElse(NO_EXT).toLowerCase(Locale.ROOT))
                    .exists(p => nestedExts.contains(p.toLowerCase(Locale.ROOT)))
            ) {
                logMsg(s"raster_to_grid -> path ext identified for nestedHandling", 2)
                true
            } else {
                false
            }
        }

        // <<< GDAL READER STRATEGY && OPTIONS >>>
        readStrat = {
            // have to go out of way to manually specify "-1"
            if (config("sizeInMB").toInt < 0) MOSAIC_RASTER_READ_AS_PATH
            else MOSAIC_RASTER_SUBDIVIDE_ON_READ
        }

        logMsg(s"raster_to_grid -> nestedHandling? $nestedHandling | nPartitions? $nPartitions | " +
            s"read strat? $readStrat", 1)
        logMsg(s"\nraster_to_grid - config (after any reader mods)? $config\n", 2)

        val baseOptions = Map(
            "extensions" -> config("extensions"),
            "vsizip" -> config("vsizip"),
            "subdatasetName" -> config("subdatasetName"),
            MOSAIC_RASTER_READ_STRATEGY -> readStrat
        )
        readOptions =
            if (driverName.nonEmpty && readStrat == MOSAIC_RASTER_SUBDIVIDE_ON_READ) {
                baseOptions +
                    ("driverName" -> driverName, "sizeInMB" -> config("sizeInMB"))
            }
            else if (driverName.nonEmpty) baseOptions + ("driverName" -> driverName)
            else if (readStrat == MOSAIC_RASTER_SUBDIVIDE_ON_READ) baseOptions + ("sizeInMB" -> config("sizeInMB"))
            else baseOptions
        logMsg(s"\nraster_to_grid - readOptions? $readOptions\n", 2)
    }

    /**
      * Get the configuration map.
      * @return
      *   The configuration map.
      */
    private def getConfig: Map[String, String] = {
        Map(
            "combiner" -> this.extraOptions.getOrElse("combiner", "mean"),
            "deltaFileRecords" -> this.extraOptions.getOrElse("deltaFileRecords", "1000"),       // <- for tables
            "driverName" -> this.extraOptions.getOrElse("driverName", ""),
            "extensions" -> this.extraOptions.getOrElse("extensions", "*"),
            "finalTableFqn" -> this.extraOptions.getOrElse("finalTableFqn", ""),                 // <- identifies use of tables
            "finalTableFuse" -> this.extraOptions.getOrElse("finalTableFuse", ""),               // <- for tables
            "keepInterimTables" -> this.extraOptions.getOrElse("keepInterimTables", "false"),    // <- for tables
            "kRingInterpolate" -> this.extraOptions.getOrElse("kRingInterpolate", "0"),
            "limitTessellate" -> this.extraOptions.getOrElse("limitTessellate", "0"),
            "nPartitions" -> this.extraOptions.getOrElse("nPartitions", sparkSession.conf.get("spark.sql.shuffle.partitions")),
            "resolution" -> this.extraOptions.getOrElse("resolution", "0"),
            "retile" -> this.extraOptions.getOrElse("retile", "false"),
            "sizeInMB" -> this.extraOptions.getOrElse("sizeInMB", "0"),
            "skipProject" -> this.extraOptions.getOrElse("skipProject", "false"),                // <- debugging primarily
            "stepTessellate" -> this.extraOptions.getOrElse("stepTessellate", "false"),
            "stopAtTessellate" -> this.extraOptions.getOrElse("stopAtTessellate", "false"),      // <- debugging + tessellate perf
            "subdatasetName" -> this.extraOptions.getOrElse("subdatasetName", ""),
            "tileSize" -> this.extraOptions.getOrElse("tileSize", "512"),
            "toTif" -> this.extraOptions.getOrElse("toTif", "false"),                            // <- tessellate perf
            "uriDeepCheck" -> this.extraOptions.getOrElse("uriDeepCheck", "false"),
            "verboseLevel" -> this.extraOptions.getOrElse("verboseLevel", "0"),
            "vsizip" -> this.extraOptions.getOrElse("vsizip", "false")
        )
    }

    /**
     * "raster_to_grid" reader can be very long-running depending on the data size.
     * This is a consolidated function for providing interim information during processing.
     * Messages shows up in stdout in driver logs, if they are at/below `verboseLevel`. The
     * higher the level, the more granular the information.
     *
     * @param msg
     *   Message to log.
     * @param level
     *   Level of the information (0..2)
     */
    private def logMsg(msg: String, level: Int): Unit = {
        if (level <= verboseLevel) {
            //scalastyle:off println
            println(msg)
            //scalastyle:on println
        }
    }

}
