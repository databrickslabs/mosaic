package com.databricks.labs.gbx.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.reflect.{ClassTag, classTag}

object SerializationUtil {

    private def dtOf[T: ClassTag]: DataType =
        classTag[T] match {
            case t if t == classTag[UTF8String]  => StringType
            case t if t == classTag[String]      => StringType
            case t if t == classTag[Boolean]     => BooleanType
            case t if t == classTag[Byte]        => ByteType
            case t if t == classTag[Short]       => ShortType
            case t if t == classTag[Int]         => IntegerType
            case t if t == classTag[Long]        => LongType
            case t if t == classTag[Float]       => FloatType
            case t if t == classTag[Double]      => DoubleType
            case t if t == classTag[Array[Byte]] => BinaryType
            // Add more if needed (e.g., DateType via java.sql.Date, TimestampType via java.sql.Timestamp)
            case _                               => throw new IllegalArgumentException(s"Unsupported type ${classTag[T].runtimeClass}")
        }

    def createMap_StringString(mapData: MapData): Map[String, String] = {
        val keys = mapData.keyArray().toArray[String](StringType)
        val values = mapData.valueArray().toArray[String](StringType)
        (keys zip values).collect { case (k: String, v: String) => k -> v }.toMap
    }

    def createMap[K: ClassTag, V: ClassTag](mapData: MapData): Map[K, V] = {
        def createArray[T: ClassTag](data: ArrayData, dt: DataType): Array[T] = {
            if (classTag[T] == classTag[String]) data.toArray[UTF8String](dt).map(_.toString).asInstanceOf[Array[T]]
            else data.toArray[T](dt)
        }
        val kt = dtOf[K]
        val vt = dtOf[V]
        val keys = createArray[K](mapData.keyArray(), kt)
        val values = createArray[V](mapData.valueArray(), vt)
        (keys zip values).map { case (k, v) => k -> v }.toMap
    }

    def toMapData[K: ClassTag, V: ClassTag](mtd: Map[K, V]): MapData = {
        ArrayBasedMapData(
          mtd.iterator,
          mtd.size,
          (k: Any) => if (dtOf[K] == StringType) UTF8String.fromString(k.toString) else k.asInstanceOf[K],
          (v: Any) => if (dtOf[V] == StringType) UTF8String.fromString(v.toString) else v.asInstanceOf[V]
        )
    }

    def create2DArray[T: ClassTag](kernelAD: ArrayData, kdt: DataType): Array[Array[T]] = {
        val rows = kernelAD.numElements()
        val cols = kernelAD.getArray(0).numElements()
        val result = Array.ofDim[T](rows, cols)
        for (i <- 0 until rows) {
            result(i) = create1DArray[T](kernelAD.getArray(i), kdt)
        }
        result
    }

    def create1DArray[T: ClassTag](arrayData: ArrayData, dt: DataType): Array[T] = {
        if (classTag[T] == classTag[String]) {
            arrayData.toArray[UTF8String](dt).map(_.toString).asInstanceOf[Array[T]]
        } else {
            arrayData.toArray[T](dt)
        }
    }

    def createRow(values: Seq[Any]): InternalRow = {
        InternalRow.fromSeq(
            values.map {
                case null           => null
                case b: Array[Byte] => b
                case v: Array[_]    => new GenericArrayData(v)
                case m: Map[_, _]   => toMapData[String, String](m.map { case (k, v) => (k.toString, v.toString) })
                case s: String      => UTF8String.fromString(s)
                case v              => v
            }
        )
    }

}
