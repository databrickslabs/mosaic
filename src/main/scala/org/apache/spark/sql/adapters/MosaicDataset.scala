package org.apache.spark.sql.adapters

import org.apache.spark.sql.{DataFrame, Dataset}

class MosaicDataset(df: DataFrame) extends Dataset(df.queryExecution, df.encoder) {}
