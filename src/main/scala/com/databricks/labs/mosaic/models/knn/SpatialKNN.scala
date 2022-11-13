package com.databricks.labs.mosaic.models.knn

import com.databricks.labs.mosaic.functions._
import com.databricks.labs.mosaic.models.core.IterativeTransformer
import com.databricks.labs.mosaic.models.util.CheckpointManager
import org.apache.spark.internal.Logging
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util._
import org.apache.spark.ml.Transformer
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * A transformer that takes a DataFrame with a column of geometries and returns
  * a DataFrame with matching geometries and their neighbours from the right
  * DataFrame. The right DataFrame must have a column of geometries. The
  * transformer is configured using the [[SpatialKNNParams]] class.
  * @param uid
  *   A unique identifier for the transformer. This is used to identify the
  *   transformer when saving and loading the transformer. The default value is
  *   [[Identifiable.randomUID]].
  * @param rightDf
  *   The right DataFrame to join with. This DataFrame must have a column of
  *   geometries.
  */
class SpatialKNN(override val uid: String, var rightDf: Dataset[_])
    extends Transformer
      with IterativeTransformer
      with SpatialKNNParams
      with DefaultParamsWritable
      with Logging {

    // The MosaicContext is used to access the functions in the Mosaic library.
    private val mc = MosaicContext.context()
    // The HexRingNeighbours transformer is used to find the neighbours of each geometry.
    // It is configured using the parameters of this transformer.
    private val hexRingNeighboursTf = HexRingNeighbours(rightDf)
    // The checkpoint manager is used to manage the checkpointing of the interim matches.
    // We are using delta checkpoints to avoid the overhead of writing the entire DataFrame and unneeded unions.
    var matchesCheckpoint: CheckpointManager = _
    import mc.functions._
    // A variable Dataset used to store current state of the matching process.
    private var matches: Dataset[_] = _
    // The number of current matches.
    private var matchCount: Long = 0

    /**
      * A default constructor that is needed for loading the transformer. Note
      * that loaded transformers will not have a right DataFrame. And right
      * DataFrame must be set before the transformer can be used.
      */
    def this() = this(Identifiable.randomUID("ApproximateSpatialKNN"), null)

    /**
      * A constructor that is needed for loading the transformer. Note that
      * loaded transformers will not have a right DataFrame. And right DataFrame
      * must be set before the transformer can be used.
      */
    def this(uid: String) = this(uid, null)

    /**
      * A copy constructor that takes a new right DataFrame. The right DataFrame
      * must have a column of geometries. This is used to emulate the behaviour
      * of the setParamName methods in the [[Params]] API.
      * @param right
      *   The right DataFrame to join with using KNN relation.
      * @return
      *   A copy of this transformer with the right DataFrame set.
      */
    def setRightDf(right: Dataset[_]): this.type = {
        this.rightDf = right
        hexRingNeighboursTf.setRight(right)
        this
    }

    /**
      * Transform the input dataset before the first iteration. Make sure
      * left_miid field exists. left_miid is the default value for
      * [[leftRowID]]. This can be changed using the [[setLeftRowID]] method.
      * left_miid is used to identify the left geometry in the result. Set the
      * starting iteration to 1.
      * @param input
      *   Input dataset
      * @return
      *   Transformed dataset
      */
    override def inputTransform(input: Dataset[_]): DataFrame = {
        input
            .withColumn(getLeftRowID, monotonically_increasing_id())
            .withColumn("iteration", lit(1))
            .withColumn("match_radius", lit(0.0))
    }

    /**
      * Early stopping condition. Stop when the number of matches doesnt change
      * for [[getEarlyStopping]] iterations or when the number of candidates
      * left to match doesnt change for [[getEarlyStopping]] iterations.
      * @param preDf
      *   Dataset containing the previous candidates left to match.
      * @param postDf
      *   Dataset containing the current candidates left to match.
      * @return
      *   True if early stopping condition is met, false otherwise
      */
    override def earlyStoppingCheck(preDf: Dataset[_], postDf: Dataset[_]): Boolean = {
        val preCount = preDf.count()
        val postCount = postDf.count()
        val newMatchesCount = matchesCheckpoint
            .load()
            .where("right_miid is not null")
            .groupBy("left_miid", "right_miid")
            .count()
            .count()
        val condition = (preCount == postCount) && (matchCount == newMatchesCount) && (newMatchesCount > 0)
        matchCount = newMatchesCount
        condition
    }

    /**
      * Transform each interim result using the k distance of
      * [[HexRingNeighbours]] as a function of the current iteration. After each
      * iteration increment the iteration by 1.
      * @param dataset
      *   Previous iteration matching candidates from the left input dataset.
      * @return
      *   Current iteration matching candidates from the left input dataset.
      *   Only the rows that do not have sufficient neighbours are returned.
      */
    override def iterationTransform(dataset: Dataset[_]): DataFrame = {
        // We are using delta checkpoints to avoid the overhead of writing the entire DataFrame and unneeded unions.
        // When checkpoints are overwrite only then .checkpoint(eager=true) is a better option.
        val newMatches = hexRingNeighboursTf.transform(dataset)
        matches = matchesCheckpoint.append(newMatches)

        val groupByCols = dataset.columns.filterNot(Seq("iteration", "match_radius", getLeftRowID).contains)
        val result = matches
            // Apply distance threshold but dont drop null matches
            .where(coalesce(col(hexRingNeighboursTf.distanceCol) <= getDistanceThreshold, lit(true)))
            .groupBy(getLeftRowID, groupByCols: _*)
            .agg(
              sum(col(getRightRowID).isNotNull.cast("int")).alias("match_count"),
              max("iteration").alias("iteration"),
              max(hexRingNeighboursTf.distanceCol).alias("match_radius")
            )
            // Null matches are still counted, so we need to subtract 1
            .where(col("match_count") - 1 < getKNeighbours)
            .drop("match_count")

        hexRingNeighboursTf
            .setIterationID(hexRingNeighboursTf.getIterationID + 1)

        // No need to checkpoint result, it will be check-pointed by IterativeTransformer.
        result
    }

    /**
      * Transform the result dataset of [[getMaxIterations]] iteration
      * transformations. The final iteration is needed to adjust final result
      * for offsets that happen within the centroid grid cell. One additional
      * hex ring is needed to account for that offset.
      * @param result
      *   Result dataset of [[getMaxIterations]] iteration transformations.
      * @return
      *   Adjusted result dataset.
      */
    override def resultTransform(result: Dataset[_]): DataFrame = {
        // Final iteration logic is encoded with iterationID == -1.
        // The final iteration uses Kth neighbour distance for the match radius.
        if (getApproximate) {
            result.toDF()
        } else {
            val finalInput = result
                .withColumn("iteration", col("iteration") + 1)
                .withColumnRenamed("match_radius", hexRingNeighboursTf.distanceCol)
            hexRingNeighboursTf
                .setIterationID(-1)
            iterationTransform(finalInput)        }
    }

    /**
      * Transform the input dataset. The input dataset must have a column of
      * geometries. The output dataset contains the left geometry, the right
      * geometry and the distance between the two geometries. It also contains
      * the iteration when the match occurred and the order number of the
      * neighbour based on the distance.
      * @param dataset
      *   Input dataset
      * @return
      *   Matches dataset
      */
    override def transform(dataset: Dataset[_]): DataFrame = {
        matchesCheckpoint = CheckpointManager(getCheckpointTablePrefix, "matches", isTable = getUseTableCheckpoint)

        val rightDfIndexed = rightDf
            .withColumn(getRightRowID, monotonically_increasing_id())
            .withColumn(
              hexRingNeighboursTf.rightGridCol(getRightFeatureCol),
              grid_tessellateexplode(col(getRightFeatureCol), getIndexResolution, keepCoreGeometries = false)
            )
            .checkpoint(true)

        hexRingNeighboursTf
            .setRight(rightDfIndexed)
            .setLeftFeatureCol(getLeftFeatureCol)
            .setRightFeatureCol(getRightFeatureCol)
            .setIndexResolution(getIndexResolution)
            .setIterationID(1)

        iterate(dataset)

        matches = matchesCheckpoint
            .load()
            // Drop null matches (needed during iterations for proper hex ring neighbour calculation).
            .where(col("right_miid").isNotNull)
            .withColumn("neighbour_number", row_number().over(hexRingNeighboursTf.window))
            .where(col("neighbour_number") <= getKNeighbours)
            // Apply distance threshold
            .where(col(hexRingNeighboursTf.distanceCol) <= getDistanceThreshold)

        matches = matchesCheckpoint.overwrite(matches)
        matches.toDF()
    }

    /**
      * Generate the output schema. The output schema contains the left schema,
      * the projected right schema, left_miid, right_miid, distance, iteration
      * and neighbour_number.
      * @return
      *   Left geometry column name
      */
    override def transformSchema(schema: StructType): StructType = {
        val neighboursSchema = hexRingNeighboursTf.transformSchema(schema).copy()
        neighboursSchema
            .add(getLeftRowID, LongType, nullable = false)
            .add(getRightRowID, LongType, nullable = true)
            .add("iteration", IntegerType, nullable = false)
            .add("neighbour_number", IntegerType, nullable = false)
    }

    override def copy(extra: ParamMap): SpatialKNN = defaultCopy(extra)

    /**
      * @return
      *   Parameters of this transformer as a Seq of key value pairs.
      */
    def getParams: Seq[(String, String)] = {
        val params = this.params
            .map { param =>
                this.get(param) match {
                    case Some(value) => (param.name, value.toString)
                    case None        => (param.name, "None")
                }
            }
            .filterNot(_._2 == "None")
        params
    }

    /**
      * Computes the set of metrics to evaluate the model. We track number of
      * matches, number of iterations, min max range of neighbours over all rows
      * in the input dataset and min max range of distance radius for captured
      * matches over all rows in the input dataset.
      * @return
      *   A Seq of metrics.
      */
    def getMetrics: Seq[(String, Double)] = {
        val matches = matchesCheckpoint.load()
        val matchCount = matches.count().toDouble
        val maxNeighbours = matches
            .groupBy(getLeftRowID)
            .agg(max("neighbour_number"))
            .agg(max("max(neighbour_number)"))
            .first()
            .getInt(0)
            .toDouble
        val maxRadius = matches
            .groupBy(getLeftRowID)
            .agg(max(hexRingNeighboursTf.distanceCol).alias("max(distance)"))
            .agg(max("max(distance)"))
            .first()
            .getDouble(0)
        val minRadius = matches
            .groupBy(getLeftRowID)
            .agg(min(hexRingNeighboursTf.distanceCol).alias("min(distance)"))
            .agg(min("min(distance)"))
            .first()
            .getDouble(0)
        val convergenceIteration = matches
            .groupBy(getLeftRowID)
            .agg(max("iteration"))
            .agg(max("max(iteration)"))
            .first()
            .getInt(0)
            .toDouble

        val metrics = Seq(
          ("match_count", matchCount),
          ("max_neighbours", maxNeighbours),
          ("max_radius", maxRadius),
          ("min_radius", minRadius),
          ("convergence_iteration", convergenceIteration)
        )
        metrics
    }

}

/**
  * A companion object for the [[SpatialKNN]] transformer. Implements
  * the apply constructor for the [[SpatialKNN]] transformer.
  * Implements the default reader for the [[SpatialKNN]] transformer.
  */
object SpatialKNN extends DefaultParamsReadable[SpatialKNN] {

    def apply(rightDf: Dataset[_]): SpatialKNN =
        new SpatialKNN(Identifiable.randomUID("approximate_spatial_knn"), rightDf)

}
