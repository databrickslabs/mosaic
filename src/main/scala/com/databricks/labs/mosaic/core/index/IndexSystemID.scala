package com.databricks.labs.mosaic.core.index

sealed trait IndexSystemID {
    def name: String
}

object IndexSystemID {

    def apply(name: String): IndexSystemID =
        name match {
            case "H3"  => H3
            case _  => throw new NotImplementedError("Index not supported yet!") // scalastyle:ignore
        }

    def getIndexSystem(indexSystemID: IndexSystemID): IndexSystem =
        indexSystemID match {
            case H3  => H3IndexSystem
            case _  => throw new NotImplementedError("Index not supported yet!") // scalastyle:ignore
        }

}

case object H3 extends IndexSystemID {
    override def name: String = "H3"
}
