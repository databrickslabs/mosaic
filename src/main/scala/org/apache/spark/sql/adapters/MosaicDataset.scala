package org.apache.spark.sql.adapters

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.{FilterFunction, MapFunction}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types.{Metadata, StructType}
import org.apache.spark.sql.{Column, DataFrame, DataFrameNaFunctions, DataFrameStatFunctions, DataFrameWriter, DataFrameWriterV2, Dataset, Encoder, KeyValueGroupedDataset, MergeIntoWriter, Observation, RelationalGroupedDataset, Row, SparkSession, TypedColumn}
import org.apache.spark.storage.StorageLevel

import java.util
import scala.reflect.runtime.universe

class MosaicDataset(df: DataFrame) extends Dataset() {
    override val sparkSession: SparkSession = ???
    override val encoder: Encoder[Nothing] = ???

    override def queryExecution: QueryExecution = ???

    override def toDF(): DataFrame = ???

    override def as[U: Encoder]: Dataset[U] = ???

    override def to(schema: StructType): DataFrame = ???

    override def toDF(colNames: String*): DataFrame = ???

    override def schema: StructType = ???

    override def explain(mode: String): Unit = ???

    override def isLocal: Boolean = ???

    override def isEmpty: Boolean = ???

    override def isStreaming: Boolean = ???

    override protected def checkpoint(eager: Boolean, reliableCheckpoint: Boolean, storageLevel: Option[StorageLevel]): Dataset[Nothing] = ???

    override def withWatermark(eventTime: String, delayThreshold: String): Dataset[Nothing] = ???

    override def show(numRows: Int, truncate: Boolean): Unit = ???

    override def show(numRows: Int, truncate: Int, vertical: Boolean): Unit = ???

    override def na: DataFrameNaFunctions = ???

    override def stat: DataFrameStatFunctions = ???

    override def join(right: Dataset[_]): DataFrame = ???

    override def join(right: Dataset[_], usingColumns: Seq[String], joinType: String): DataFrame = ???

    override def join(right: Dataset[_], joinExprs: Column, joinType: String): DataFrame = ???

    override def crossJoin(right: Dataset[_]): DataFrame = ???

    override def joinWith[U](other: Dataset[U], condition: Column, joinType: String): Dataset[(Nothing, U)] = ???

    override def lateralJoin(right: Dataset[_]): DataFrame = ???

    override def lateralJoin(right: Dataset[_], joinExprs: Column): DataFrame = ???

    override def lateralJoin(right: Dataset[_], joinType: String): DataFrame = ???

    override def lateralJoin(right: Dataset[_], joinExprs: Column, joinType: String): DataFrame = ???

    override protected def sortInternal(global: Boolean, sortExprs: Seq[Column]): Dataset[Nothing] = ???

    override def hint(name: String, parameters: Any*): Dataset[Nothing] = ???

    override def col(colName: String): Column = ???

    override def metadataColumn(colName: String): Column = ???

    override def colRegex(colName: String): Column = ???

    override def as(alias: String): Dataset[Nothing] = ???

    override def select(cols: Column*): DataFrame = ???

    override def select[U1](c1: TypedColumn[Nothing, U1]): Dataset[U1] = ???

    override protected def selectUntyped(columns: TypedColumn[_, _]*): Dataset[_] = ???

    override def filter(condition: Column): Dataset[Nothing] = ???

    override def filter(func: Nothing => Boolean): Dataset[Nothing] = ???

    override def filter(func: FilterFunction[Nothing]): Dataset[Nothing] = ???

    override def groupBy(cols: Column*): RelationalGroupedDataset = ???

    override def rollup(cols: Column*): RelationalGroupedDataset = ???

    override def cube(cols: Column*): RelationalGroupedDataset = ???

    override def groupingSets(groupingSets: Seq[Seq[Column]], cols: Column*): RelationalGroupedDataset = ???

    override def reduce(func: (Nothing, Nothing) => Nothing): Nothing = ???

    override def groupByKey[K: Encoder](func: Nothing => K): KeyValueGroupedDataset[K, Nothing] = ???

    override def unpivot(ids: Array[Column], values: Array[Column], variableColumnName: String, valueColumnName: String): DataFrame = ???

    override def unpivot(ids: Array[Column], variableColumnName: String, valueColumnName: String): DataFrame = ???

    override def transpose(indexColumn: Column): DataFrame = ???

    override def transpose(): DataFrame = ???

    override def scalar(): Column = ???

    override def exists(): Column = ???

    override def observe(name: String, expr: Column, exprs: Column*): Dataset[Nothing] = ???

    override def observe(observation: Observation, expr: Column, exprs: Column*): Dataset[Nothing] = ???

    override def limit(n: Int): Dataset[Nothing] = ???

    override def offset(n: Int): Dataset[Nothing] = ???

    override def union(other: Dataset[Nothing]): Dataset[Nothing] = ???

    override def unionByName(other: Dataset[Nothing], allowMissingColumns: Boolean): Dataset[Nothing] = ???

    override def intersect(other: Dataset[Nothing]): Dataset[Nothing] = ???

    override def intersectAll(other: Dataset[Nothing]): Dataset[Nothing] = ???

    override def except(other: Dataset[Nothing]): Dataset[Nothing] = ???

    override def exceptAll(other: Dataset[Nothing]): Dataset[Nothing] = ???

    override def sample(withReplacement: Boolean, fraction: Double, seed: Long): Dataset[Nothing] = ???

    override def randomSplit(weights: Array[Double], seed: Long): Array[Dataset[Nothing]] = ???

    override def randomSplitAsList(weights: Array[Double], seed: Long): util.List[Dataset[Nothing]] = ???

    override def randomSplit(weights: Array[Double]): Array[Dataset[Nothing]] = ???

    override def explode[A <: Product : universe.TypeTag](input: Column*)(f: Row => IterableOnce[A]): DataFrame = ???

    override def explode[A, B: universe.TypeTag](inputColumn: String, outputColumn: String)(f: A => IterableOnce[B]): DataFrame = ???

    override private[spark] def withColumns(colNames: Seq[String], cols: Seq[Column]): DataFrame = ???

    override protected def withColumnsRenamed(colNames: Seq[String], newColNames: Seq[String]): DataFrame = ???

    override def withMetadata(columnName: String, metadata: Metadata): DataFrame = ???

    override def drop(colNames: String*): DataFrame = ???

    override def drop(col: Column, cols: Column*): DataFrame = ???

    override def dropDuplicates(): Dataset[Nothing] = ???

    override def dropDuplicates(colNames: Seq[String]): Dataset[Nothing] = ???

    override def dropDuplicatesWithinWatermark(): Dataset[Nothing] = ???

    override def dropDuplicatesWithinWatermark(colNames: Seq[String]): Dataset[Nothing] = ???

    override def describe(cols: String*): DataFrame = ???

    override def summary(statistics: String*): DataFrame = ???

    override def head(n: Int): Array[Nothing] = ???

    override def map[U: Encoder](func: Nothing => U): Dataset[U] = ???

    override def map[U](func: MapFunction[Nothing, U], encoder: Encoder[U]): Dataset[U] = ???

    override def mapPartitions[U: Encoder](func: Iterator[Nothing] => Iterator[U]): Dataset[U] = ???

    override def foreachPartition(f: Iterator[Nothing] => Unit): Unit = ???

    override def tail(n: Int): Array[Nothing] = ???

    override def collect(): Array[Nothing] = ???

    override def collectAsList(): util.List[Nothing] = ???

    override def toLocalIterator(): util.Iterator[Nothing] = ???

    override def count(): Long = ???

    override def repartition(numPartitions: Int): Dataset[Nothing] = ???

    override protected def repartitionByExpression(numPartitions: Option[Int], partitionExprs: Seq[Column]): Dataset[Nothing] = ???

    override protected def repartitionByRange(numPartitions: Option[Int], partitionExprs: Seq[Column]): Dataset[Nothing] = ???

    override def coalesce(numPartitions: Int): Dataset[Nothing] = ???

    override def persist(): Dataset[Nothing] = ???

    override def cache(): Dataset[Nothing] = ???

    override def persist(newLevel: StorageLevel): Dataset[Nothing] = ???

    override def storageLevel: StorageLevel = ???

    override def unpersist(blocking: Boolean): Dataset[Nothing] = ???

    override def unpersist(): Dataset[Nothing] = ???

    override protected def createTempView(viewName: String, replace: Boolean, global: Boolean): Unit = ???

    override def mergeInto(table: String, condition: Column): MergeIntoWriter[Nothing] = ???

    override def writeStream: DataStreamWriter[Nothing] = ???

    override def writeTo(table: String): DataFrameWriterV2[Nothing] = ???

    override def toJSON: Dataset[String] = ???

    override def inputFiles: Array[String] = ???

    override def sameSemantics(other: Dataset[Nothing]): Boolean = ???

    override def semanticHash(): Int = ???

    override def write: DataFrameWriter[Nothing] = ???

    override def rdd: RDD[Nothing] = ???

    override def toJavaRDD: JavaRDD[Nothing] = ???
}
