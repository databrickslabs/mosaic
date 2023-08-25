package org.apache.spark.sql.adapters

import org.apache.spark.sql.SparkSession

class DataFrameReader(sparkSession: SparkSession) extends org.apache.spark.sql.DataFrameReader(sparkSession) {}
