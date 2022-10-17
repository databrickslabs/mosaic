package com.databricks.labs.mosaic.models

import com.databricks.labs.mosaic.functions._
import org.apache.spark.internal.Logging
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructType}

import scala.util.control.Breaks.{break, breakable}
import scala.util.Try

class ApproximateSpatialKNN(override val uid: String)
    extends Transformer
      with ApproximateSpatialKNNParams
      with DefaultParamsWritable
      with Logging {

    private val mc = MosaicContext.context()
    // Count doesnt depend on order, so we are using unordered window
    private var rightDf: Dataset[_] = _

    private var projectedRightDf: Dataset[_] = _
    private var projectedLeftFeature: String = _
    private var projectedRightFeature: String = _

    def this() = this(Identifiable.randomUID("approximate_spatial_knn"))

    def setRightDf(df: Dataset[_]): this.type = {
        rightDf = df
        this
    }

    def inferResolution(): Unit = {
        val spark = SparkSession.builder().getOrCreate()
        import mc.functions._
        import spark.implicits._

        val k = getKNeighbours
        val resolutions = mc.getIndexSystem.resolutions

        if (rightDf.count() / k < 10000) {
            val midRes = resolutions.toSeq(resolutions.size / 2)
            setIndexResolution(midRes)
            return
        }

        breakable {
            for (i <- resolutions.toSeq.sorted) yield {
                val countPercentiles = rightDf
                    .sample(0.1)
                    .withColumn("right_miid", monotonically_increasing_id())
                    .withColumn("grid_cell", grid_tessellateexplode(col(getRightFeatureCol), i))
                    .groupBy("grid_cell")
                    .count()
                    .select(
                      percentile_approx(col("count"), lit(0.25), lit(1)).as("q1"),
                      percentile_approx(col("count"), lit(0.5), lit(1)).as("median"),
                      percentile_approx(col("count"), lit(0.75), lit(1)).as("q3")
                    )
                    .as[(Double, Double, Double)]
                    .first()

                // avoid division by 0
                val q1ratio = k / (countPercentiles._1 + 0.01)
                val q2ratio = k / (countPercentiles._2 + 0.01)
                val q3ratio = k / (countPercentiles._3 + 0.01)

                if (q1ratio < 10.0 || q2ratio < 10.0 || q3ratio < 10.0) {
                    setIndexResolution(i)
                    break
                }

            }
        }

        // If the process doesnt converge to a resolution, we set it to the mid resolution
        if (Try(getIndexResolution).isFailure) {
            val midRes = resolutions.toSeq(resolutions.size / 2)
            setIndexResolution(midRes)
        }
    }

    override def transform(dataset: Dataset[_]): DataFrame = {
        import mc.functions._

        // Path locations only used for testing
        val checkpointManager = CheckpointManager(getCheckpointTablePrefix, isTable = !getIsTest)

        val sharedSubSchema = dataset.columns.intersect(rightDf.columns)
        val leftUniqueSchema = dataset.columns.diff(sharedSubSchema)
        val rightUniqueSchema = rightDf.columns.diff(sharedSubSchema)
        val leftSharedSchema = sharedSubSchema.diff(leftUniqueSchema)
        val rightSharedSchema = sharedSubSchema.diff(rightUniqueSchema)

        val newLeftSchema = leftUniqueSchema.map(col) ++ leftSharedSchema.map(cn => col(cn).alias(s"left_$cn"))
        val newRightSchema = rightUniqueSchema.map(col) ++ rightSharedSchema.map(cn => col(cn).alias(s"right_$cn"))

        val projectedDataset = dataset.select(newLeftSchema: _*)
        projectedRightDf = rightDf.select(newRightSchema: _*)

        projectedLeftFeature = if (leftUniqueSchema.contains(getLeftFeatureCol)) getLeftFeatureCol else s"left_${getLeftFeatureCol}"
        projectedRightFeature = if (rightUniqueSchema.contains(getRightFeatureCol)) getRightFeatureCol else s"right_${getRightFeatureCol}"
        val rightGridCol = s"${projectedRightFeature}_grid"

        val nIterations = getMaxIterations
        var tmpDf = projectedDataset
            .withColumn("left_miid", monotonically_increasing_id())

        var convergenceCount = 0
        var earlyStoppingCount = 0

        var rightDfIndexed = projectedRightDf
            .withColumn("right_miid", monotonically_increasing_id())
            .withColumn(rightGridCol, grid_tessellateexplode(col(projectedRightFeature), getIndexResolution, keepCoreGeometries = false))

        // checkpoint the table to avoid recomputation
        // since we cant be sure of table size in advance we avoid caching
        checkpointManager.deleteIfExists()
        checkpointManager.overwriteCheckpoint(rightDfIndexed, "rightDfIndexed")
        rightDfIndexed = checkpointManager.loadFromCheckpoint("rightDfIndexed").toDF()
        //SparkSession.builder().getOrCreate().sql(s"drop table if exists ${getCheckpointTablePrefix}_rightDfIndexed")
        //rightDfIndexed.write.format("delta").saveAsTable(s"${getCheckpointTablePrefix}_rightDfIndexed")
        //rightDfIndexed = SparkSession.builder().getOrCreate().read.table(s"${getCheckpointTablePrefix}_rightDfIndexed")

        logInfo("Right DF Indexed stored at: " + s"${getCheckpointTablePrefix}_rightDfIndexed")

        breakable {
            logInfo("Starting KNN iterations")
            for (i <- 0 until nIterations) yield {
                generateMatches(tmpDf, rightDfIndexed, i, checkpointManager)
                val oldLeftToMatchCount = tmpDf.count()
                tmpDf.unpersist()

                val matches = checkpointManager.loadFromCheckpoint("matches").toDF()
                // Intersection between matches for i=n and i=n+1 is an empty set
                // sum across multiple iterations is the total number of matches

                tmpDf = matches
                    // in scala dataset.columns: _* wont work
                    .groupBy("left_miid", projectedDataset.columns: _*)
                    .agg(
                      sum(when(col("right_miid").isNotNull, 1).otherwise(0)).as("match_count")
                    )
                    .where(col("match_count") < getKNeighbours)
                    .drop("match_count")
                    .cache()

                val leftToMatchCount = tmpDf.count()

                logInfo(s"Iteration $i completed. Left to match count: $leftToMatchCount")

                // Allow one more iteration after convergence to make sure we are not taking wrong decision
                // for the last few matches due to kring/kdisc approximations.
                if (convergenceCount == 1) break
                if (leftToMatchCount == 0) convergenceCount += 1

                // Early stopping if the number of matches is not decreasing.
                // If early stopping isnt provided it defaults to maxIterations.
                if (oldLeftToMatchCount == leftToMatchCount) earlyStoppingCount += 1
                if (earlyStoppingCount == getEarlyStopping) break

            }
        }

        val matches =
            checkpointManager.loadFromCheckpoint("matches").toDF()
            .where(col("right_miid").isNotNull)
            // After convergence the row_number needs to be recomputed over ordered window.
            .withColumn("neighbour_number", row_number().over(window))
            .where(col("neighbour_number") <= getKNeighbours)

        matches
    }

    private def generateMatches(dataset: Dataset[_], rightDfIndexed: Dataset[_], iteration: Int, checkpointManager: CheckpointManager): Unit = {
        import mc.functions._

        val leftKringCol = s"${projectedLeftFeature}_kring"
        val rightGridCol = s"${projectedRightFeature}_grid"

        // We are iterating from 0, but KRings only make sense from k >= 1
        val k = iteration + 1

        // first iteration will use KRing neighbourhood
        // subsequent iterations will use KDisc neighbourhood
        val rippleFunction = (geom: Column, resolution: Int) =>
            if (k == 1) grid_geometrykringexplode(geom, resolution, iteration) else grid_geometrykdiscexplode(geom, resolution, k)

        val leftDfIndexed = dataset
            .withColumn(leftKringCol, rippleFunction(col(projectedLeftFeature), getIndexResolution))
            .withColumn(leftKringCol, grid_wrapaschip(col(leftKringCol), isCore = true, getCellGeom = false))

        val matchesAggSchema = (dataset.columns ++ projectedRightDf.columns)
            .filterNot(Seq("left_miid", "right_miid").contains)
            .map(cn => first(cn).alias(cn))
        val matches = leftDfIndexed
            .join(rightDfIndexed, col(leftKringCol).getItem("index_id") === col(rightGridCol).getItem("index_id"), joinType = "left_outer")
            .groupBy("left_miid", "right_miid")
            .agg(
              // in scala variable arguments are passed as Seq.head, Seq.tail :_*
              // so we need to pass the first element of the Seq and then the rest of the Seq
              st_intersects_aggregate(col(leftKringCol), col(rightGridCol)).alias("intersects"),
              matchesAggSchema: _*
            )
            // keep all matches with intersects but dont drop no matches cases
            .where("(intersects is null) or intersects")
            // remove self matches but dont drop no matches cases
            .drop("intersects", leftKringCol, rightGridCol)
            .withColumn(distanceCol, st_distance(col(projectedLeftFeature), col(projectedRightFeature)))
            // Avoid feature matching itself
            .where(col(distanceCol) > 0 || (hash(col(projectedLeftFeature)) !== hash(col(projectedRightFeature))))
            .where(col(distanceCol) <= getDistanceThreshold)
            .withColumn("iteration", lit(iteration))
            .withColumn("neighbour_number", row_number().over(window))

        //saveToCheckpoint(matches, iteration)
        if (iteration == 0) {
            checkpointManager.deleteIfExists()
            checkpointManager.overwriteCheckpoint(matches, "matches")
        } else {
            checkpointManager.appendToCheckpoint(matches, "matches")
        }

    }

    private def window = Window.partitionBy("left_miid").orderBy(asc(distanceCol))

    private def saveToCheckpoint(dataset: Dataset[_], iteration: Int): Unit = {
        if (iteration == 0) {
            SparkSession.builder().getOrCreate().sql(s"drop table if exists ${getCheckpointTablePrefix}_rightDfIndexed")
        }
        dataset.write
            .mode(SaveMode.Append)
            .format("delta")
            .saveAsTable(s"${getCheckpointTablePrefix}_matches")
    }

    private def loadFromCheckpoint(): Dataset[_] = {
        SparkSession
            .builder()
            .getOrCreate()
            .read
            .format("delta")
            .load(s"${getCheckpointTablePrefix}_matches")
    }

    override def transformSchema(schema: StructType): StructType = {
        val newSchema = new StructType()
        newSchema.add(distanceCol, DoubleType)
        schema.fields.foreach(newSchema.add)
        rightDf.schema.fields.foreach(newSchema.add)
        newSchema
    }

    private def distanceCol = s"${getLeftFeatureCol}_${getRightFeatureCol}_distance"

    override def copy(extra: ParamMap): ApproximateSpatialKNN = defaultCopy(extra)

}
