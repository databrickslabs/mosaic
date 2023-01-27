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
  * a DataFrame with matching geometries and their neighbours from the
  * candidates DataFrame. The candidates DataFrame must have a column of
  * geometries. The transformer is configured using the [[SpatialKNNParams]]
  * class.
  * @param uid
  *   A unique identifier for the transformer. This is used to identify the
  *   transformer when saving and loading the transformer. The default value is
  *   [[Identifiable.randomUID]].
  * @param candidatesDf
  *   The candidates DataFrame to join with. This DataFrame must have a column
  *   of geometries.
  */
class SpatialKNN(override val uid: String, var candidatesDf: Dataset[_])
    extends Transformer
      with IterativeTransformer
      with SpatialKNNParams
      with DefaultParamsWritable
      with Logging {

    // The MosaicContext is used to access the functions in the Mosaic library.
    private val mc = MosaicContext.context()
    // The HexRingNeighbours transformer is used to find the neighbours of each geometry.
    // It is configured using the parameters of this transformer.
    private val hexRingNeighboursTf = GridRingNeighbours(candidatesDf)
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
      * that loaded transformers will not have a candidates DataFrame. And
      * candidates DataFrame must be set before the transformer can be used.
      */
    def this() = this(Identifiable.randomUID("ApproximateSpatialKNN"), null)

    /**
      * A constructor that is needed for loading the transformer. Note that
      * loaded transformers will not have a candidates DataFrame. And candidates
      * DataFrame must be set before the transformer can be used.
      */
    def this(uid: String) = this(uid, null)

    /**
      * A copy constructor that takes a new candidates DataFrame. The candidates
      * DataFrame must have a column of geometries. This is used to emulate the
      * behaviour of the setParamName methods in the [[Params]] API.
      * @param candidatesDf
      *   The candidates DataFrame to join with using KNN relation.
      * @return
      *   A copy of this transformer with the candidates DataFrame set.
      */
    def setCandidatesDf(candidatesDf: Dataset[_]): this.type = {
        this.candidatesDf = candidatesDf
        hexRingNeighboursTf.setRight(candidatesDf)
        this
    }

    /**
      * Transform the input dataset before the first iteration. Make sure
      * landmarks_miid field exists. landmarks_miid is the default value for
      * [[landmarksRowID]]. This can be changed using the [[setLandmarksRowID]]
      * method. landmarks_miid is used to identify the landmarks dataset rows in
      * the result. Set the starting iteration to 1. Set the starting match
      * radius to 0.0.
      * @param input
      *   Input dataset
      * @return
      *   Transformed dataset
      */
    override def inputTransform(input: Dataset[_]): DataFrame = {
        input
            .withColumn(getLandmarksRowID, monotonically_increasing_id())
            .withColumn("iteration", lit(1))
            .withColumn("match_radius", lit(0.0))
    }

    /**
      * Early stopping condition. Stop when the number of matches doesn't change
      * for [[getEarlyStopIterations]] iterations or when the number of
      * candidates left to match doesn't change for [[getEarlyStopIterations]]
      * iterations.
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
            .where(col(getCandidatesRowID).isNotNull)
            .groupBy(getLandmarksRowID, getCandidatesRowID)
            .count()
            .count()
        val shouldStop = (preCount == postCount) && (matchCount == newMatchesCount) && (newMatchesCount > 0)
        matchCount = newMatchesCount
        shouldStop
    }

    /**
      * Transform each interim result using the k distance of
      * [[GridRingNeighbours]] as a function of the current iteration. After
      * each iteration increment the iteration by 1.
      *
      * @param dataset
      *   Previous iteration matching candidates from the landmarks input
      *   dataset.
      * @return
      *   Current iteration matching candidates from the landmarks input
      *   dataset. Only the rows that do not have sufficient neighbours are
      *   returned.
      */
    override def iterationTransform(dataset: Dataset[_]): DataFrame = {
        // We are using delta checkpoints to avoid the overhead of writing the entire DataFrame and unneeded unions.
        // When checkpoints are overwrite only then .checkpoint(eager=true) is a better option.
        val newMatches = hexRingNeighboursTf.transform(dataset)
        matches = matchesCheckpoint.append(newMatches)

        val groupByCols = dataset.columns.filterNot(Seq("iteration", "match_radius", getLandmarksRowID).contains)
        val result = matches
            // Apply distance threshold but dont drop null matches
            .where(coalesce(col(hexRingNeighboursTf.distanceCol) <= getDistanceThreshold, lit(true)))
            .groupBy(getLandmarksRowID, groupByCols: _*)
            .agg(
              // col(getCandidatesRowID).isNotNull.cast("int") = 1 when rowId is not null and 0 otherwise.
              // sum(col(getCandidatesRowID).isNotNull.cast("int")) is used to count the number of neighbours for each row.
              // Note that count("*") would not return the correct count.
              sum(col(getCandidatesRowID).isNotNull.cast("int")).alias("match_count"),
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
            iterationTransform(finalInput)
        }
    }

    /**
      * Transform the input dataset. The input dataset must have a column of
      * geometries. The output dataset contains the landmark geo features, the
      * candidates geo features and the distance between the two geometries. It
      * also contains the iteration when the match occurred and the order number
      * of the neighbour based on the distance.
      * @param dataset
      *   Input dataset
      * @return
      *   Matches dataset
      */
    override def transform(dataset: Dataset[_]): DataFrame = {
        matchesCheckpoint = CheckpointManager(getCheckpointTablePrefix, "matches", isTable = getUseTableCheckpoint)

        val candidatesDfIndexed = candidatesDf
            .withColumn(getCandidatesRowID, monotonically_increasing_id())
            .withColumn(
              hexRingNeighboursTf.rightGridCol(getCandidatesFeatureCol),
              grid_tessellateexplode(col(getCandidatesFeatureCol), getIndexResolution, keepCoreGeometries = false)
            )
            .checkpoint(true)

        hexRingNeighboursTf
            .setRight(candidatesDfIndexed)
            .setRightFeatureCol(getCandidatesFeatureCol)
            .setRightRowID(getCandidatesRowID)
            .setLeftFeatureCol(getLandmarksFeatureCol)
            .setLeftRowID(getLandmarksRowID)
            .setIndexResolution(getIndexResolution)
            .setIterationID(1)

        iterate(dataset)

        matches = matchesCheckpoint
            .load()
            // Drop null matches (needed during iterations for proper hex ring neighbour calculation).
            .where(col(getCandidatesRowID).isNotNull)
            .withColumn("neighbour_number", row_number().over(hexRingNeighboursTf.window))
            .where(col("neighbour_number") <= getKNeighbours)
            // Apply distance threshold
            .where(col(hexRingNeighboursTf.distanceCol) <= getDistanceThreshold)

        matches = matchesCheckpoint.overwrite(matches)
        matches.toDF()
    }

    /**
      * Generate the output schema. The output schema contains the landmarks
      * dataset schema, the projected candidates dataset schema, landmarks_miid,
      * candidates_miid, distance, iteration and neighbour_number.
      * @return
      *   Output dataset schema.
      */
    override def transformSchema(schema: StructType): StructType = {
        val neighboursSchema = hexRingNeighboursTf.transformSchema(schema).copy()
        neighboursSchema
            .add(getLandmarksRowID, LongType, nullable = false)
            .add(getCandidatesRowID, LongType, nullable = true)
            .add(hexRingNeighboursTf.distanceCol, DoubleType, nullable = false)
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
            .groupBy(getLandmarksRowID)
            .agg(max("neighbour_number"))
            .agg(max("max(neighbour_number)"))
            .first()
            .getInt(0)
            .toDouble
        val maxRadius = matches
            .groupBy(getLandmarksRowID)
            .agg(max(hexRingNeighboursTf.distanceCol).alias("max(distance)"))
            .agg(max("max(distance)"))
            .first()
            .getDouble(0)
        val minRadius = matches
            .groupBy(getLandmarksRowID)
            .agg(min(hexRingNeighboursTf.distanceCol).alias("min(distance)"))
            .agg(min("min(distance)"))
            .first()
            .getDouble(0)
        val convergenceIteration = matches
            .groupBy(getLandmarksRowID)
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
  * A companion object for the [[SpatialKNN]] transformer. Implements the apply
  * constructor for the [[SpatialKNN]] transformer. Implements the default
  * reader for the [[SpatialKNN]] transformer.
  */
object SpatialKNN extends DefaultParamsReadable[SpatialKNN] {

    def apply(candidatesDf: Dataset[_]): SpatialKNN = new SpatialKNN(Identifiable.randomUID("spatial_knn"), candidatesDf)

}
