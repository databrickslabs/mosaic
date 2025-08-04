package org.apache.spark.sql.adapters

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.util.Properties

class DataFrameReader(sparkSession: SparkSession) extends org.apache.spark.sql.DataFrameReader() {
    override def load(): DataFrame = ???

    override def load(path: String): DataFrame = ???

    override def load(paths: String*): DataFrame = ???

    override def jdbc(url: String, table: String, predicates: Array[String], connectionProperties: Properties): DataFrame = ???

    override def json(jsonDataset: Dataset[String]): DataFrame = ???

    override def json(jsonRDD: JavaRDD[String]): DataFrame = ???

    override def json(jsonRDD: RDD[String]): DataFrame = ???

    override def csv(csvDataset: Dataset[String]): DataFrame = ???

    override def xml(xmlDataset: Dataset[String]): DataFrame = ???

    override def table(tableName: String): DataFrame = ???
}
