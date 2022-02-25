package com.databricks.mosaic.sql

object SQLExceptions {
  def NotEnoughGeometriesException: Exception = new Exception(
    "Not enough geometries supplied to MosaicAnalyser to compute resolution metrics. " +
      "Try increasing the sampleFraction using the setSampleFraction method.")
}
