package com.databricks.labs.mosaic.models.knn

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.models.core.BinaryTransformer
import org.apache.spark.internal.Logging
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * A transformer that takes a DataFrame with a column of geometries and returns
  * a DataFrame with matching geometries and their neighbours from the right
  * DataFrame. The right DataFrame must have a column of geometries. The
  * transformer is configured using the [[GridRingNeighboursParams]] class.
  *
  * @param uid
  *   A unique identifier for the transformer. This is used to identify the
  *   transformer when saving and loading the transformer. The default value is
  *   [[Identifiable.randomUID]].
  * @param right
  *   The right DataFrame to join with. This DataFrame must have a column of
  *   geometries.
  */
case class GridRingNeighbours(override val uid: String, var right: Dataset[_])
    extends BinaryTransformer
      with GridRingNeighboursParams
      with DefaultParamsWritable
      with Logging {

    private val mc = MosaicContext.context()
    import mc.functions._

    /**
      * A copy constructor that takes a new right DataFrame. The right DataFrame
      * must have a column of geometries.
      *
      * @param right
      *   The right DataFrame to join with.
      */
    def setRight(right: Dataset[_]): this.type = {
        this.right = right
        this
    }

    /** @return A column name for the kring column of the left DataFrame. */
    def leftKringCol: String = s"left_${getLeftFeatureCol}_kring"

    /**
      * @return
      *   A column name for the grid index column of the right DataFrame.
      */
    def rightGridCol: String = rightGridCol(getRightFeatureCol)

    def rightGridCol(rightFeatureCol: String): String = s"right_${rightFeatureCol}_grid"

    /**
      * Transforms the left dataset before the join is performed. The left
      * transformations are: <ul> <li>If the iteration is 1: Add a column with
      * the kring of the left geometries.</li> <li>If the iteration is greater
      * than 1: Add a column with the kdisc of the left geometries.</li> <li>If
      * the iteration is == -1: Add last iteration logic that ensures exactness
      * of the neighbour candidates. The last iteration is based on the buffered
      * geometry by the distance between geometry and Kth neighbour candidate.
      * We will use this buffer to check for any neighbor closer than current
      * Kth neighbour that we may have missed due to grid cell distortions.</li>
      * <li>Wrap the generated index IDs into logical chips.</li> </ul>
      * @param left
      *   The left DataFrame to transform.
      * @return
      *   The transformed left DataFrame.
      */
    override def leftTransform(left: Dataset[_]): DataFrame = {
        // Ensure logical projection doesnt break leftTransform
        val featureCol = left.columns.find(_ == getLeftFeatureCol).getOrElse(s"left_$getLeftFeatureCol")
        val i = getIterationID
        val res = getIndexResolution
        val result =
            if (i == -1) {
                left
                    .withColumn(s"max_$distanceCol", max(distanceCol).over(window))
                    .withColumn("max_iteration", max("iteration").over(window))
                    .withColumn("iteratedCells", grid_geometrykring(col(featureCol), res, col("max_iteration")))
                    .withColumn("missedCandidateCells", grid_tessellate(st_buffer(col(featureCol), col(s"max_$distanceCol")), res))
                    .withColumn("missedCandidateCells", col("missedCandidateCells").getField("chips").getField("index_id"))
                    .withColumn(leftKringCol, explode(array_except(col("missedCandidateCells"), col("iteratedCells"))))
                    .drop(s"max_$distanceCol", "max_iteration", "iteratedCells", "missedCandidateCells")
            } else if (i == 1) {
                left.withColumn(leftKringCol, grid_geometrykringexplode(col(featureCol), res, i))
            } else {
                left.withColumn(leftKringCol, grid_geometrykloopexplode(col(featureCol), res, i))
            }

        result
            .withColumn(leftKringCol, grid_wrapaschip(col(leftKringCol), isCore = true, getCellGeom = false))
    }

    /**
      * Transforms the right dataset before the join is performed. The right
      * transformations are: <ul> <li>This function is used as a passthrough
      * function. Since we will cache pre-transformed right DataFrame in the
      * iteration transformer.</li> </ul>
      * @param right
      *   The right DataFrame to transform.
      * @return
      *   The transformed right DataFrame.
      */
    override def rightTransform(right: Dataset[_]): DataFrame = right.toDF()

    /**
      * Transforms the left DataFrame with the right DataFrame. The join is
      * performed using the left kring column and the right grid index column.
      * @param dataset
      *   The left DataFrame to join with.
      * @return
      *   The joined DataFrame.
      */
    override def transform(dataset: Dataset[_]): DataFrame =
        binaryTransform(
          dataset,
          right,
          // index_id is safe here since mosaic functions always
          // have index_id as a part of chip sub schema
          col(leftKringCol).getItem("index_id") === col(rightGridCol).getItem("index_id"),
          how = "left_outer"
        )

    override def resultTransform(result: DataFrame): DataFrame = {
        // Note .get is safe here since we want to fail if the column is not present.
        val projectedRightFeature = result.columns
            .find(cn => cn == s"right_$getRightFeatureCol")
            .getOrElse(
              result.columns.find(_ == getRightFeatureCol).get
            )
        val matchesAggSchema = result.columns
            .filterNot(Seq(getLeftRowID, getRightRowID).contains)
            .map(cn => first(cn).alias(cn))

        result
            .groupBy(getLeftRowID, getRightRowID)
            .agg(
              // in scala variable arguments are passed as Seq.head, Seq.tail :_*
              // so we need to pass the first element of the Seq and then the rest of the Seq
              st_intersects_aggregate(col(leftKringCol), col(rightGridCol)).alias("intersects"),
              matchesAggSchema: _*
            )
            // keep all matches with intersects but dont drop no matches cases
            .where(coalesce(col("intersects"), lit(true)))
            .drop("intersects", leftKringCol, rightGridCol)
            .withColumn(distanceCol, st_distance(col(getLeftFeatureCol), col(projectedRightFeature)))
            .withColumn("is_self_match", hash(col(getLeftFeatureCol)) === hash(col(projectedRightFeature)))
            .where(!col("is_self_match"))
            .withColumn("neighbour_number", row_number().over(window))
            .withColumn("iteration", if (getIterationID == -1) col("iteration") else lit(getIterationID))
            .drop("is_self_match")

    }

    override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

    override def transformSchema(schema: StructType): StructType = {
        val rightUniqueSchema = uniqueFields(right.schema, schema)
        val newSchema = new StructType()
        for (field <- schema) newSchema.add(field)
        for (field <- right.schema) {
            if (rightUniqueSchema.contains(field)) newSchema.add(field)
            else newSchema.add(field.copy(name = s"right_${field.name}"))
        }
        newSchema
            .add("distance", DoubleType, nullable = false)
            .add("neighbour_number", IntegerType, nullable = false)
    }

    def distanceCol: String = s"${getLeftFeatureCol}_${getRightFeatureCol}_distance"

    def window: WindowSpec = Window.partitionBy(getLeftRowID).orderBy(asc(distanceCol))

    /**
      * @return
      *   Parameters of this transformer.
      */
    def getParams: Seq[(String, String)] = {
        val params = this.params
            .filterNot(_.name == "iterationID")
            .map(param =>
                this.get(param) match {
                    case Some(value) => (param.name, value.toString)
                    case None        => (param.name, "None")
                }
            )
            .filterNot(_._2 == "None")
        params
    }

}

//  extends DefaultParamsReadable[GridRingNeighbours]
object GridRingNeighbours {

    def apply(right: Dataset[_]): GridRingNeighbours =
        new GridRingNeighbours(uid = Identifiable.randomUID("grid_ring_neighbours"), right = right)

}
