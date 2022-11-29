package com.databricks.labs.mosaic.core.index


case class GridConf(
                     boundXMin: Long,
                     boundXMax: Long,
                     boundYMin: Long,
                     boundYMax: Long,
                     cellSubdivisionX: Int,
                     cellSubdivisionY: Int
                   ) {
  val spanX = boundXMax - boundXMin
  val spanY = boundYMax - boundYMin

  val resBits = 8 // We keep 8 Most Significant Bits for resolution
  val idBits = 56 // The rest can be used for the cell ID

  val subCellsCount = cellSubdivisionX * cellSubdivisionY

  // We need a distinct value for each cell, plus one bit for the parent cell (all-zeroes for LSBs)
  // We compute it with log2(subCellsCount)
  val bitsPerResolution = Math.ceil(Math.log10(subCellsCount) / Math.log10(2)).toInt

  // A cell ID has to fit the reserved number of bits
  val maxResolution = Math.min(20, Math.floor(idBits / bitsPerResolution).toInt)

//  val totalCellsX = Math.pow(cellSubdivisionX, maxResolution).toLong
//  val totalCellsY = Math.pow(cellSubdivisionY, maxResolution).toLong
//
//  val totalCells = totalCellsX * totalCellsY
}