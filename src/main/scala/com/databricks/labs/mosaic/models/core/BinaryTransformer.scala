package com.databricks.labs.mosaic.models.core

import org.apache.spark.ml.Transformer
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

/**
  * BinaryTransformer is a base class for all binary transformers. For specific
  * behaviour, override the following methods: <ul> <li> leftTransform
  * (optional) - transform the left dataset before the merge, default is noop.
  * </li> <li> rightTransform (optional) - transform the right dataset before
  * the merge, default is noop. </li> <li> resultTransform (optional) -
  * transform the merged dataset after the merge, default is noop. </li> </ul>
  * If neither leftTransform nor rightTransform nor resultTransform are
  * overridden, the transformer simply applies a join using the join condition.
  */
trait BinaryTransformer extends Transformer {

    /**
      * Transform the left dataset before the merge. Default implementation is a
      * noop.
      *
      * @param left
      *   Left dataset, if this transformer is a part of the pipeline, this is
      *   the output of the previous step.
      * @return
      *   Transformed dataset.
      */
    def leftTransform(left: Dataset[_]): DataFrame

    /**
      * Transform the right dataset before the merge. Default implementation is
      * a noop.
      * @param right
      *   Right dataset, if this transformer is a part of the pipeline, this is
      *   dataframe is external to the pipeline and it is passed as a parameter
      *   to the transformer implementing this trait.
      * @return
      *   Transformed dataset.
      */
    def rightTransform(right: Dataset[_]): DataFrame

    /**
      * Transform the merged dataset after the merge. Default implementation is
      * a noop.
      * @param result
      *   Merged dataset.
      * @return
      *   Transformed dataset.
      */
    def resultTransform(result: DataFrame): DataFrame

    /**
      * Transform the left and right datasets using leftTransform and
      * rightTransform. Merge the transformed datasets using the join condition.
      * Transform the merged dataset using resultTransform.
      * @param left
      *   Left dataset. If this transformer is a part of the pipeline, this is
      *   the output of the previous step.
      * @param right
      *   Right dataset. if this transformer is a part of the pipeline, this is
      *   dataframe is external to the pipeline and it is passed as a parameter
      *   to the transformer implementing this trait.
      * @param joinCondition
      *   Condition to be used to join leftTransformed and rightTransformed
      *   datasets.
      * @param how
      *   Type of join to be used.
      * @return
      *   Transformed dataset.
      */
    def binaryTransform(left: Dataset[_], right: Dataset[_], joinCondition: Column, how: String): DataFrame = {

        val leftTransformed = leftTransform(left)
        val rightTransformed = rightTransform(right)

        val joinedDf = mergeTransform(leftTransformed, rightTransformed, joinCondition, how)

        resultTransform(joinedDf)
    }

    /**
      * Merge the left and right datasets using the join condition. Project the
      * columns of the columns of the right to make sure there are no name
      * collisions.
      * @param left
      *   Left dataset.
      * @param right
      *   Right dataset.
      * @param joinCondition
      *   Condition to be used to join left and right datasets.
      * @param how
      *   Type of join to be used.
      * @return
      *   Merged dataset.
      */
    def mergeTransform(left: Dataset[_], right: Dataset[_], joinCondition: Column, how: String): DataFrame = {
        // We keep left schema intact and add right schema is prefixed.
        val rightUniqueSchema = uniqueFields(right.schema, left.schema)
        val projectedRightDf = rightProjection(right, rightUniqueSchema.fieldNames)

        left.join(projectedRightDf, joinCondition, how)
    }

    /**
      * Identify the unique fields in the schema.
      * @param schema
      *   The main schema.
      * @param otherSchema
      *   The schema of the other dataset.
      * @return
      *   The schema of the unique fields in the main schema.
      */
    def uniqueFields(schema: StructType, otherSchema: StructType): StructType = {
        val leftColumns = schema.fieldNames
        val rightColumns = otherSchema.fieldNames

        val sharedColumns = leftColumns.intersect(rightColumns)
        val uniqueColumns = leftColumns.diff(sharedColumns)

        val uniqueSchema = StructType(uniqueColumns.map(schema.apply))

        uniqueSchema
    }

    /**
      * Project the columns of the right dataset to make sure there are no name
      * collisions.
      * @param df
      *   Right dataset.
      * @param uniqueColumns
      *   Names of the unique columns of the right dataset.
      * @return
      *   Projected dataset.
      */
    def rightProjection(df: Dataset[_], uniqueColumns: Seq[String]): DataFrame = {
        val sharedColumns = df.columns.diff(uniqueColumns)
        val toSelect = uniqueColumns.map(col) ++ sharedColumns.map(cn => col(cn).alias(s"right_$cn"))
        df.select(toSelect: _*)
    }

}
