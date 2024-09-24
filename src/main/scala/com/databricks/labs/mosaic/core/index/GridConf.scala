package com.databricks.labs.mosaic.core.index

case class GridConf(
                     boundXMin: Long,
                     boundXMax: Long,
                     boundYMin: Long,
                     boundYMax: Long,
                     cellSplits: Int,
                     rootCellSizeX: Int,
                     rootCellSizeY: Int,
                     crsID: Option[Int] = None
                   ) {
  private val spanX = boundXMax - boundXMin
  private val spanY = boundYMax - boundYMin

  val resBits = 8 // We keep 8 Most Significant Bits for resolution
  val idBits = 56 // The rest can be used for the cell ID

  //noinspection ScalaWeakerAccess
  val subCellsCount: Int = cellSplits * cellSplits

  // We need a distinct value for each cell, plus one bit for the parent cell (all-zeroes for LSBs)
  // We compute it with log2(subCellsCount)
  val bitsPerResolution: Int = Math.ceil(Math.log10(subCellsCount) / Math.log10(2)).toInt

  // A cell ID has to fit the reserved number of bits
  val maxResolution: Int = Math.min(20, Math.floor(idBits / bitsPerResolution).toInt)

  val rootCellCountX: Int = Math.ceil(spanX.toDouble / rootCellSizeX).toInt
  val rootCellCountY: Int = Math.ceil(spanY.toDouble / rootCellSizeY).toInt

}