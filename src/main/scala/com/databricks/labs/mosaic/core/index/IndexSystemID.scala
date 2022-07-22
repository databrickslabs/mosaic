package com.databricks.labs.mosaic.core.index

sealed trait IndexSystemID {
    def name: String
}

object IndexSystemID {

    def apply(name: String): IndexSystemID =
        name match {
            case "H3"  => H3
            case "BNG" => BNG
            case _     => throw new Error("Index not supported yet!")
        }

    def getIndexSystem(indexSystemID: IndexSystemID): IndexSystem =
        indexSystemID match {
            case H3  => H3IndexSystem
            case BNG => BNGIndexSystem
            case _   => throw new Error("Index not supported yet!")
        }

}

case object H3 extends IndexSystemID {
    override def name: String = "H3"
}

case object BNG extends IndexSystemID {
    override def name: String = "BNG"
}
