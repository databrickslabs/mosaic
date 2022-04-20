package com.databricks.labs.mosaic.core.types.model

case class CDMArray(
    ndArrayInt: Array[Array[Array[Int]]],
    ndArrayDbl: Array[Array[Array[Double]]]
) {}
