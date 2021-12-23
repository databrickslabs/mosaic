package com.databricks.mosaic.core

sealed trait IndexSystemID {
  def name: String
}

object IndexSystemID {

  def apply(name: String): IndexSystemID = name match {
    case "H3" => H3
    case "S2" => S2
    case "BNG" => BNG
  }

  def getIndexSystem(indexSystemID: IndexSystemID): IndexSystem = indexSystemID match {
    case H3 => H3IndexSystem
    case S2 => throw new NotImplementedError("S2 not supported yet!")
    case BNG => throw new NotImplementedError("BNG not supported yet!")
  }
}

case object H3 extends IndexSystemID {
  override def name: String = "H3"
}

case object S2 extends IndexSystemID {
  override def name: String = "S2"
} // for future support

case object BNG extends IndexSystemID {
  override def name: String = "BNG"
} // for future support