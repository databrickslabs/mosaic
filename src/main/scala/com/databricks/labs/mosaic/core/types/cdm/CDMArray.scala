package com.databricks.labs.mosaic.core.types.cdm

import org.apache.spark.sql.catalyst.util.GenericArrayData

sealed abstract class CDMArray {
    def serialize: Any
}

case class CDMArray1d[T](
    values: Array[T]
) extends CDMArray {
    override def serialize: Any = new GenericArrayData(values)
}

case class CDMArray2d[T](
    values: Array[Array[T]]
) extends CDMArray {
    override def serialize: Any = new GenericArrayData(values.map(i => new GenericArrayData(i)))
}

case class CDMArray3d[T](
    values: Array[Array[Array[T]]]
) extends CDMArray {
    override def serialize: Any = {
        new GenericArrayData(values.map(i => new GenericArrayData(i.map(j => new GenericArrayData(j)))))
    }
}
