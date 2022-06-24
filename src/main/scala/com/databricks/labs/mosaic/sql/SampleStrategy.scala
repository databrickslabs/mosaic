package com.databricks.labs.mosaic.sql

import org.apache.spark.sql.DataFrame

case class SampleStrategy(sampleFraction: Option[Double] = None, sampleRows: Option[Int] = None) {
    def transformer(df: DataFrame): DataFrame = {
        (sampleFraction, sampleRows) match {
            case (Some(d), _) if d >= 1d => df
            case (Some(d), None)         => df.sample(d)
            case (None, Some(l))         => df.limit(l)
            case (Some(_), Some(l))      => df.limit(l)
            case _                       => df
        }
    }
}
